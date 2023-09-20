import json
import logging

from flask import Flask, request

from seafevents.ex_props_handler.ex_props_task_manager import ex_props_task_manager

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route('/can-set-ex-props', methods=['POST'])
def can_set_ex_props():
    try:
        data = json.loads(request.data)
    except Exception as e:
        return {'error_msg': 'path invalid'}, 400

    path = data.get('path')
    if not path or not isinstance(path, str):
        return {'error_msg': 'path invalid'}, 400
    repo_id = data.get('repo_id')
    if not repo_id or not isinstance(repo_id, str):
        return {'error_msg': 'repo_id invalid'}, 400

    can_set, error_type = ex_props_task_manager.can_set_item(repo_id, path)
    if not can_set:
        return {
            'can_set': can_set,
            'error_type': error_type
        }

    return {'can_set': True}


@app.route('/set-folder-items-ex-props', methods=['POST'])
def set_folder_ex_props():
    try:
        data = json.loads(request.data)
    except Exception as e:
        return {'error_msg': 'path invalid'}, 400

    path = data.get('path')
    if not path or not isinstance(path, str):
        return {'error_msg': 'path invalid'}, 400
    repo_id = data.get('repo_id')
    if not repo_id or not isinstance(repo_id, str):
        return {'error_msg': 'repo_id invalid'}, 400

    can_set = ex_props_task_manager.can_set_item(repo_id, path)
    if not can_set:
        return {'error_type': 'higher_being_set'}

    resp_json = ex_props_task_manager.add_set_task(repo_id, path, data)

    return resp_json


@app.route('/query-set-ex-props-status', methods=['GET'])
def query_set_ex_props_status():
    repo_id = request.args.get('repo_id')
    path = request.args.get('path')
    if not repo_id or not path:
        return {'error_msg': 'repo_id or path invalid'}, 400
    for cur_repo_id, folder_paths in ex_props_task_manager.worker_map.items():
        if repo_id != cur_repo_id:
            continue
        if path in folder_paths:
            return {'is_finished': False}
    return {'is_finished': True}
