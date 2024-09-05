import jwt
import logging
import json

from flask import Flask, request, make_response
from seafevents.app.config import SEAHUB_SECRET_KEY
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seafevents.seasearch.index_task.index_task_manager import index_task_manager


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

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not repos:
        return {'error_msg': 'repos invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = index_task_manager.keyword_search(query, repos, count, suffixes, search_path)

    return {'results': results}, 200
