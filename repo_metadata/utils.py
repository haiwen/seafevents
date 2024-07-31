import os
import random
import math
import exifread
import requests
import json

from io import BytesIO
from urllib.parse import quote as urlquote

from seafobj import commit_mgr, fs_mgr
from seaserv import seafile_api
from seafevents.app.config import METADATA_FILE_TYPES, FILE_SERVER


def gen_fileext_type_map():
    """
    Generate previewed file extension and file type relation map.
    """
    ext_to_type = {}
    for file_type in list(METADATA_FILE_TYPES.keys()):
        for file_ext in METADATA_FILE_TYPES.get(file_type):
            ext_to_type[file_ext] = file_type

    return ext_to_type


def gen_file_get_url(token, filename):
    return '%s/files/%s/%s' % (FILE_SERVER, token, urlquote(filename))


def get_file_by_path(repo_id, path):
    file_id = seafile_api.get_file_id_by_path(repo_id, path)
    filename = os.path.basename(path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, file_id, 'download', username='sys_summary_sdoc', use_onetime=True
    )
    url = gen_file_get_url(token, filename)
    content =requests.get(url, timeout=10).content.decode()

    if content:
        content = json.loads(content)
    return content


FILEEXT_TYPE_MAP = gen_fileext_type_map()


def get_file_type_by_name(filename):
    file_ext = os.path.splitext(filename)[1][1:].lower()
    file_type = FILEEXT_TYPE_MAP.get(file_ext)
    return file_type


def get_latlng(repo_id, commit_id, obj_id):
    lat_lng_info = {
        "lat_key": "GPS GPSLatitudeRef",
        "lat_value": "GPS GPSLatitude",
        "lng_key": "GPS GPSLongitudeRef",
        "lng_value": "GPS GPSLongitude"
    }

    new_commit = commit_mgr.load_commit(repo_id, 0, commit_id)
    version = new_commit.get_version()
    f = fs_mgr.load_seafile(repo_id, version, obj_id)
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
                                        {'options': gen_select_options(list(METADATA_FILE_TYPES.keys()))})
        self.location = MetadataColumn('_location', '_location', 'geolocation', {'geo_format': 'lng_lat'})
        self.summary = MetadataColumn('_summary', '_summary', 'long-text')


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
