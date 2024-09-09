import os
import random
import math
import exifread

from io import BytesIO

from seafobj import fs_mgr

from seafevents.app.config import METADATA_FILE_TYPES
from seafevents.repo_metadata.view_data_sql import view_data_2_sql


def gen_fileext_type_map():
    """
    Generate previewed file extension and file type relation map.
    """
    ext_to_type = {}
    for file_type in list(METADATA_FILE_TYPES.keys()):
        for file_ext in METADATA_FILE_TYPES.get(file_type):
            ext_to_type[file_ext] = file_type

    return ext_to_type


FILEEXT_TYPE_MAP = gen_fileext_type_map()


def get_file_type_ext_by_name(filename):
    file_ext = os.path.splitext(filename)[1][1:].lower()
    file_type = FILEEXT_TYPE_MAP.get(file_ext)
    return file_type, file_ext


def get_latlng(repo_id, obj_id):
    lat_lng_info = {
        "lat_key": "GPS GPSLatitudeRef",
        "lat_value": "GPS GPSLatitude",
        "lng_key": "GPS GPSLongitudeRef",
        "lng_value": "GPS GPSLongitude"
    }

    f = fs_mgr.load_seafile(repo_id, 1, obj_id)
    content = f.get_content()
    exif_content = exifread.process_file(BytesIO(content))

    for key in lat_lng_info.values():
        if key not in exif_content:
            return "", ""

    lat_list = exif_content[lat_lng_info["lat_value"]].values
    lat = int(lat_list[0]) + int(lat_list[1]) / 60 + float(lat_list[2]) / 3600
    lng_list = exif_content[lat_lng_info["lng_value"]].values
    lng = int(lng_list[0]) + int(lng_list[1]) / 60 + float(lng_list[2]) / 3600
    return lat, lng


def gen_select_options(option_names):
    options = []

    id_set = set()
    for option_name in option_names:
        option_id = gen_option_id(id_set)
        options.append({'id': option_id, 'name': option_name})
        id_set.add(option_id)
    return options

def gen_file_type_options(option_ids):
    options = []

    for option_id in option_ids:
        options.append({ 'id': option_id, 'name': option_id })
    return options


def gen_option_id(id_set):
    _id = str(math.floor(random.uniform(0.1, 1) * (10 ** 6)))

    while True:
        if _id not in id_set:
            return _id
        _id = str(math.floor(random.uniform(0.1, 1) * (10 ** 6)))


class MetadataTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return MetadataColumns()


class MetadataColumns(object):
    def __init__(self):
        self.id = MetadataColumn('_id', '_id', 'text')
        self.file_creator = MetadataColumn('_file_creator', '_file_creator', 'text')
        self.file_ctime = MetadataColumn('_file_ctime', '_file_ctime', 'date')
        self.file_modifier = MetadataColumn('_file_modifier', '_file_modifier', 'text')
        self.file_mtime = MetadataColumn('_file_mtime', '_file_mtime', 'date')
        self.parent_dir = MetadataColumn('_parent_dir', '_parent_dir', 'text')
        self.file_name = MetadataColumn('_name', '_name', 'text')
        self.is_dir = MetadataColumn('_is_dir', '_is_dir', 'checkbox')
        self.file_type = MetadataColumn('_file_type', '_file_type', 'single-select',
                                        {'options': gen_file_type_options(list(METADATA_FILE_TYPES.keys()))})
        self.location = MetadataColumn('_location', '_location', 'geolocation', {'geo_format': 'lng_lat'})
        self.obj_id = MetadataColumn('_obj_id', '_obj_id', 'text')
        self.size = MetadataColumn('_size', '_size', 'number')
        self.suffix = MetadataColumn('_suffix', '_suffix', 'text')
        self.file_details = MetadataColumn('_file_details', '_file_details', 'long-text')
        self.image_feature = MetadataColumn('_image_feature', '_image_feature', 'long-text')


class MetadataColumn(object):
    def __init__(self, key, name, type, data=None):
        self.key = key
        self.name = name
        self.type = type
        self.data = data

    def to_dict(self):
        column_data = {
            'key': self.key,
            'name': self.name,
            'type': self.type,
        }
        if self.data:
            column_data['data'] = self.data

        return column_data


METADATA_TABLE = MetadataTable('0001', 'Table1')


def gen_view_data_sql(table, columns, view, start, limit, username = '', id_in_org = ''):
    return view_data_2_sql(table, columns, view, start, limit, username, id_in_org)

