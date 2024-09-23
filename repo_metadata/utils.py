import os
import random
import math
import exiftool
import tempfile

from seafobj import commit_mgr, fs_mgr

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


def get_file_content(repo_id, commit_id, obj_id):
    new_commit = commit_mgr.load_commit(repo_id, 0, commit_id)
    version = new_commit.get_version()
    f = fs_mgr.load_seafile(repo_id, version, obj_id)
    content = f.get_content()
    return content


def get_image_details(content):
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file.write(content)
        temp_file.flush()
        temp_file_path = temp_file.name
        with exiftool.ExifTool() as et:
            metadata = et.get_metadata(temp_file_path)
            capture_time = metadata['EXIF:DateTimeOriginal'].replace(':', '-', 2) if metadata.get('EXIF:DateTimeOriginal') else ''
            focal_length = str(metadata['EXIF:FocalLength']) + 'mm' if metadata.get('EXIF:FocalLength') else ''
            f_number = 'f/' + str(metadata['EXIF:FNumber']) if metadata.get('EXIF:FNumber') else ''
            details = {
                'Dimensions': str(metadata.get('File:ImageWidth')) + 'x' + str(metadata.get('File:ImageHeight')),
                'Device make': metadata.get('EXIF:Make', ''),
                'Device model': metadata.get('EXIF:Model', ''),
                'Color space': metadata.get('ICC_Profile:ColorSpaceData', ''),
                'Capture time': capture_time,
                'Focal length': focal_length,
                'F number': f_number,
                'Exposure time': metadata.get('EXIF:ExposureTime', ''),
            }
            for k, v in metadata.items():
                if k.startswith('XMP'):
                    details[k[4:]] = v

            lat = metadata.get('EXIF:GPSLatitude')
            lng = metadata.get('EXIF:GPSLongitude')
            location = {
                'lat': lat,
                'lng': lng,
            } if lat is not None and lng is not None else {}
            return details, location


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


def get_metadata_by_obj_ids(repo_id, obj_ids, metadata_server_api):
    sql = f'SELECT * FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.obj_id.name}` IN ('
    parameters = []

    for obj_id in obj_ids:
        sql += '?, '
        parameters.append(obj_id)

    if not parameters:
        return []
    sql = sql.rstrip(', ') + ');'
    query_result = metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

    if not query_result:
        return []

    return query_result


def query_metadata_rows(repo_id, metadata_server_api, sql):
    rows = []
    offset = 10000
    start = 0

    while True:
        query_sql = f"{sql} LIMIT {start}, {offset}"
        response_rows = metadata_server_api.query_rows(repo_id, query_sql, []).get('results', [])
        if not response_rows:
            response_rows = []
        rows.extend(response_rows)
        if len(response_rows) < offset:
            break
        start += offset

    return rows


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
        self.description = MetadataColumn('_description', '_description', 'long-text')

        self.collaborator = MetadataColumn('_collaborators', '_collaborators', 'collaborator')
        self.owner = MetadataColumn('_owner', '_owner', 'collaborator')


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

