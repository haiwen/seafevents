import jwt
import logging

from flask import Flask, request, make_response
from seafevents.app.config import SEAHUB_SECRET_KEY
from seafevents.seafevent_server.task_manager import task_manager

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
