import io
import json
import os
import pytz
import random
import math
import exiftool
import tempfile

from datetime import timedelta, timezone, datetime

from PIL import Image, UnidentifiedImageError
from seafobj import commit_mgr, fs_mgr

from seafevents.app.config import METADATA_FILE_TYPES
from seafevents.repo_metadata.view_data_sql import view_data_2_sql, sort_data_2_sql
from seafevents.utils import timestamp_to_isoformat_timestr
from seafevents.repo_metadata.constants import PrivatePropertyKeys, METADATA_OP_LIMIT, METADATA_TABLE


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


def is_valid_datetime(date_string, format):
    try:
        datetime.strptime(date_string, format)
        return True
    except ValueError:
        return False


def get_file_content(repo_id, obj_id, limit=-1):
    f = fs_mgr.load_seafile(repo_id, 1, obj_id)
    content = f.get_content(limit)
    return content


def get_image_details(content):
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file.write(content)
        temp_file.flush()
        temp_file_path = temp_file.name
        with exiftool.ExifTool() as et:
            metadata = et.get_metadata(temp_file_path)
            time_zone_str = metadata.get('EXIF:OffsetTimeOriginal', '')
            capture_time = metadata.get('EXIF:DateTimeOriginal', '')
            if is_valid_datetime(capture_time, '%Y:%m:%d %H:%M:%S'):
                capture_time = datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                if time_zone_str:
                    hours, minutes = map(int, time_zone_str.split(':'))
                    tz_offset = timedelta(hours=hours, minutes=minutes)
                    tz = timezone(tz_offset)
                    capture_time = capture_time.replace(tzinfo=tz)
                    capture_time = capture_time.isoformat()
                else:
                    capture_time = timestamp_to_isoformat_timestr(capture_time.timestamp())
            else:
                capture_time = ''
            focal_length = str(metadata['EXIF:FocalLength']) + 'mm' if metadata.get('EXIF:FocalLength') else ''
            f_number = 'f/' + str(metadata['EXIF:FNumber']) if metadata.get('EXIF:FNumber') else ''
            width = metadata.get('File:ImageWidth')
            height = metadata.get('File:ImageHeight')
            if not width or not height:
                try:
                    img = Image.open(io.BytesIO(content))
                    width, height = img.size
                except UnidentifiedImageError as e:
                    pass
            dimensions = str(width) + 'x' + str(height) if width and height else ''
            details = {
                'Dimensions': dimensions,
                'Device make': metadata.get('EXIF:Make', ''),
                'Device model': metadata.get('EXIF:Model', ''),
                'Color space': metadata.get('ICC_Profile:ColorSpaceData', ''),
                'Capture time': capture_time,
                'Focal length': focal_length,
                'F number': f_number,
                'Exposure time': metadata.get('EXIF:ExposureTime', ''),
            }
            for k, v in metadata.items():
                if k.startswith('XMP') and k != 'XMP:XMPToolkit':
                    details[k[4:]] = v

            lat = metadata.get('EXIF:GPSLatitude')
            lng = metadata.get('EXIF:GPSLongitude')
            location = {
                'lat': lat,
                'lng': lng,
            } if lat is not None and lng is not None else {}
            return details, location


def get_video_details(content):
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file.write(content)
        temp_file.flush()
        temp_file_path = temp_file.name
        with exiftool.ExifTool() as et:
            metadata = et.get_metadata(temp_file_path)
            lat = metadata.get('Composite:GPSLatitude')
            lng = metadata.get('Composite:GPSLongitude')
            software = metadata.get('QuickTime:Software', '')
            capture_time = metadata.get('QuickTime:CreateDate', '')
            if is_valid_datetime(capture_time, '%Y:%m:%d %H:%M:%S'):
                capture_time = datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                capture_time = capture_time.replace(tzinfo=pytz.utc)
                capture_time = capture_time.isoformat()
            else:
                capture_time = ''
            details = {
                'Dimensions': str(metadata.get('QuickTime:SourceImageWidth')) + 'x' + str(metadata.get('QuickTime:SourceImageHeight')),
                'Duration': str(metadata.get('QuickTime:Duration')),
            }
            if capture_time:
                details['Capture time'] = capture_time
            if software:
                details['Encoding software'] = software

            location = {
                'lat': lat,
                'lng': lng,
            } if lat is not None and lng is not None else {}
            return details, location


def add_file_details(repo_id, obj_ids, metadata_server_api, face_recognition_manager=None):
    all_updated_rows = []
    query_result = get_metadata_by_obj_ids(repo_id, obj_ids, metadata_server_api)
    if not query_result:
        return []

    if face_recognition_manager and face_recognition_manager.check_face_recognition_status(repo_id):
        rows = [row for row in query_result if not row.get(METADATA_TABLE.columns.face_vectors.name) and face_recognition_manager.is_support_format(row.get(METADATA_TABLE.columns.suffix.name))]
        if rows:
            face_recognition_manager.face_embeddings(repo_id, rows)

    obj_id_to_rows = {}
    for item in query_result:
        obj_id = item[METADATA_TABLE.columns.obj_id.name]
        if obj_id not in obj_id_to_rows:
            obj_id_to_rows[obj_id] = []
        obj_id_to_rows[obj_id].append(item)

    updated_rows = []
    columns = metadata_server_api.list_columns(repo_id, METADATA_TABLE.id).get('columns', [])
    capture_time_column = [column for column in columns if column.get('key') == PrivatePropertyKeys.CAPTURE_TIME]
    has_capture_time_column = True if capture_time_column else False
    for row in query_result:
        file_type = row.get(METADATA_TABLE.columns.file_type.name)
        suffix = row.get(METADATA_TABLE.columns.suffix.name)
        need_update_file_type = False
        if not file_type:
            file_name = row.get(METADATA_TABLE.columns.file_name.name)
            file_type, suffix = get_file_type_ext_by_name(file_name)
            need_update_file_type = True
        row_id = row[METADATA_TABLE.columns.id.name]
        obj_id = row[METADATA_TABLE.columns.obj_id.name]

        limit = 100000 if suffix == 'mp4' else -1
        content = get_file_content(repo_id, obj_id, limit)
        if file_type == '_picture':
            update_row = add_image_detail_row(row_id, content, has_capture_time_column)
        elif file_type == '_video':
            update_row = add_video_detail_row(row_id, content, has_capture_time_column)
        else:
            continue
        if need_update_file_type:
            update_row[METADATA_TABLE.columns.file_type.name] = file_type
            update_row[METADATA_TABLE.columns.suffix.name] = suffix
        updated_rows.append(update_row)

        if len(updated_rows) >= METADATA_OP_LIMIT:
            metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
            all_updated_rows.extend(updated_rows)
            updated_rows = []

    if updated_rows:
        metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
        all_updated_rows.extend(updated_rows)

    return all_updated_rows


def add_image_detail_row(row_id, content, has_capture_time_column):
    image_details, location = get_image_details(content)
    update_row = {
        METADATA_TABLE.columns.id.name: row_id,
        METADATA_TABLE.columns.location.name: {'lng': location.get('lng', ''), 'lat': location.get('lat', '')},
        METADATA_TABLE.columns.file_details.name: f'\n\n```json\n{json.dumps(image_details)}\n```\n\n\n',
    }

    if has_capture_time_column:
        capture_time = image_details.get('Capture time')
        if capture_time:
            update_row[PrivatePropertyKeys.CAPTURE_TIME] = capture_time

    return update_row


def add_video_detail_row(row_id, content, has_capture_time_column):
    video_details, location = get_video_details(content)

    update_row = {
        METADATA_TABLE.columns.id.name: row_id,
        METADATA_TABLE.columns.location.name: {'lng': location.get('lng', ''), 'lat': location.get('lat', '')},
        METADATA_TABLE.columns.file_details.name: f'\n\n```json\n{json.dumps(video_details)}\n```\n\n\n',
    }

    if has_capture_time_column:
        capture_time = video_details.get('Capture time')
        if capture_time:
            update_row[PrivatePropertyKeys.CAPTURE_TIME] = capture_time

    return update_row


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


def get_metadata_by_row_ids(repo_id, row_ids, metadata_server_api):
    sql = f'SELECT * FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.id.name}` IN ('
    parameters = []

    for row_id in row_ids:
        sql += '?, '
        parameters.append(row_id)

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


def gen_view_data_sql(table, columns, view, start, limit, params):
    """ generate view data sql """
    return view_data_2_sql(table, columns, view, start, limit, params)


def gen_sorts_sql(table, columns, sorts):
    """ generate sorts sql """
    return sort_data_2_sql(table, columns, sorts)
