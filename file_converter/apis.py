import json
import logging
import os
import jwt
from pathlib import Path

from flask import request, Flask

from seafevents.file_converter.sdoc_converter import md2sdoc
from seafevents.file_converter.markdown_converter import sdoc2md
from seafevents.file_converter.utils import get_file_by_token, upload_file_by_token
from seafevents.app.config import SECRET_KEY

logger = logging.getLogger(__name__)
flask_app = Flask(__name__)

def check_auth_token(req):
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() != 'token' or len(auth) != 2:
        return False

    token = auth[1]
    if not token:
        return False

    private_key = SECRET_KEY
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        return False

    return True


@flask_app.route('/api/v1/file-convert/', methods=['POST'])
def convert_markdown_to_sdoc():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    path = data.get('path')
    username = data.get('username')
    doc_uuid = data.get('doc_uuid')

    extension = Path(path).suffix
    if extension not in ['.md', '.sdoc']:
        return {'error_msg': 'path invalid.'}, 400

    download_token = data.get('download_token')
    upload_token = data.get('upload_token')

    file_content = get_file_by_token(path, download_token).decode()

    parent_dir = os.path.dirname(path)
    file_name = os.path.basename(path)

    if extension == '.md':
        if file_content:
            file_content = md2sdoc(file_content, username=username)
        file_name = file_name[:-2] + 'sdoc'
    else:
        if file_content:
            file_content = json.loads(file_content)
            file_content = sdoc2md(file_content, doc_uuid=doc_uuid)
        file_name = file_name[:-4] + 'md'

    try:
        resp = upload_file_by_token(parent_dir, file_name, upload_token, file_content)
        if not resp.ok:
            logger.error(resp.text)
            return {'error_msg': resp.text}, 500

    except Exception as e:
        logger.error(e)
        error_msg = 'Internal Server Error'
        return {'error_msg': error_msg}, 500

    return {'success': True}, 200
