import jwt
import logging
import json

from flask import Flask, request, make_response
from seafevents.app.config import SEAHUB_SECRET_KEY
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seafevents.seasearch.index_task.index_task_manager import index_task_manager
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.utils import add_file_details

app = Flask(__name__)
logger = logging.getLogger(__name__)


def check_auth_token(req):
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() != 'token' or len(auth) != 2:
        return False, 'Token invalid.'

    token = auth[1]
    if not token:
        return False, 'Token invalid.'

    private_key = SEAHUB_SECRET_KEY
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        return False, e

    return True, None


@app.route('/add-init-metadata-task', methods=['GET'])
def add_init_metadata_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        logger.warning('seafevent server busy, queue size: %d' % (task_manager.tasks_queue.qsize(), ))
        return make_response(('seafevent server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')

    try:
        task_id = task_manager.add_init_metadata_task(
            username, repo_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-export-log-task', methods=['GET'])
def get_sys_logs_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if event_export_task_manager.tasks_queue.full():
        logger.warning('seafevent server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (event_export_task_manager.tasks_queue.qsize(), event_export_task_manager.current_task_info,
                                    event_export_task_manager.threads_is_alive()))
        return make_response(('seafevent server busy,.', 400))

    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    log_type = request.args.get('log_type')
    try:
        task_id = event_export_task_manager.add_export_logs_task(start_time, end_time, log_type)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/add-org-export-log-task', methods=['GET'])
def get_org_logs_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if event_export_task_manager.tasks_queue.full():
        logger.warning('seafevent server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (event_export_task_manager.tasks_queue.qsize(), event_export_task_manager.current_task_info,
                                    event_export_task_manager.threads_is_alive()))
        return make_response(('seafevent server busy,.', 400))

    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    log_type = request.args.get('log_type')
    org_id = request.args.get('org_id')
    try:
        task_id = event_export_task_manager.add_org_export_logs_task(start_time, end_time, log_type, org_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/query-export-status', methods=['GET'])
def query_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not event_export_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id not found.', 404))

    try:
        is_finished, error = event_export_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return make_response((e, 500))

    if error:
        return make_response((error, 500))
    return make_response(({'is_finished': is_finished}, 200))


@app.route('/search', methods=['POST'])
def search():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403

    # Check seasearch is enable
    if not index_task_manager.enabled:
        return {'error_msg': 'Seasearch is not enabled by seafevents.conf'}
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query = data.get('query').strip()
    repos = data.get('repos')
    suffixes = data.get('suffixes')
    search_path = data.get('search_path')
    obj_type = data.get('obj_type')

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not repos:
        return {'error_msg': 'repos invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = index_task_manager.file_search(query, repos, count, suffixes, search_path, obj_type)

    return {'results': results}, 200


@app.route('/add-init-face-recognition-task', methods=['GET'])
def add_init_face_recognition_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        logger.warning('seafevent server busy, queue size: %d' % (task_manager.tasks_queue.qsize(),))
        return make_response(('seafevent server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')

    try:
        task_id = task_manager.add_init_face_recognition_task(
            username, repo_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/extract-file-details', methods=['POST'])
def extract_file_details():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403

    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    obj_ids = data.get('obj_ids')
    repo_id = data.get('repo_id')

    if not obj_ids or not isinstance(obj_ids, list):
        return {'error_msg': 'obj_ids invalid.'}, 400
    if not repo_id:
        return {'error_msg': 'repo_id invalid.'}, 400

    metadata_server_api = MetadataServerAPI('seafevents')
    details = add_file_details(repo_id, obj_ids, metadata_server_api)

    return {'details': details}, 200


@app.route('/wiki-search', methods=['POST'])
def search_wiki():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403

    # Check seasearch is enable
    if not index_task_manager.enabled:
        return {'error_msg': 'Seasearch is not enabled by seafevents.conf'}
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query = data.get('query').strip()
    wiki = data.get('wiki')

    if not query:
        return {'error_msg': 'query invalid.'}, 400
    if not wiki:
        return {'error_msg': 'wiki invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results, total = index_task_manager.wiki_search(query, wiki, count)

    return {'results': results, 'total': total}, 200


@app.route('/add-convert-wiki-task', methods=['GET'])
def add_convert_wiki_task():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403

    new_repo_id = request.args.get('new_repo_id')
    old_repo_id = request.args.get('old_repo_id')
    username = request.args.get('username')

    if not new_repo_id:
        return {'error_msg': 'new_repo_id invalid.'}, 400

    if not old_repo_id:
        return {'error_msg': 'old_repo_id invalid.'}, 400

    if not username:
        return {'error_msg': 'username invalid.'}, 400

    try:
        task_id = event_export_task_manager.add_convert_wiki_task(old_repo_id, new_repo_id, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return {'task_id': task_id}, 200


@app.route('/update-people-cover-photo', methods=['POST'])
def update_cover_photo():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    repo_id = data.get('repo_id')
    people_id = data.get('people_id')
    obj_id = data.get('obj_id')

    if not repo_id:
        return {'error_msg': 'repo_id invalid.'}, 400
    if not people_id:
        return {'error_msg': 'people_id invalid.'}, 400
    if not obj_id:
        return {'error_msg': 'obj_id invalid.'}, 400

    try:
        app.face_recognition_manager.update_people_cover_photo(repo_id, people_id, obj_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return {'success': True}, 200
