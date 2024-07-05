import jwt
import logging

from flask import Flask, request, make_response
from seafevents.app.config import SEAHUB_SECRET_KEY
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager


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
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')

    try:
        task_id = task_manager.add_init_metadata_task(
            username, repo_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-init-export-log-task', methods=['GET'])
def get_sys_logs_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if event_export_task_manager.tasks_queue.full():
        logger.warning('seafile io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (event_export_task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    event_export_task_manager.threads_is_alive()))
        return make_response(('seafile io server busy.', 400))

    tstart = request.args.get('tstart')
    tend = request.args.get('tend')
    log_type = request.args.get('log_type')
    try:
        task_id = event_export_task_manager.add_export_logs_task(tstart, tend, log_type)
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

