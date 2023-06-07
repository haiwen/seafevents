import json
import logging

from flask import Flask, request

from seafevents.seaf_io.task_manager import task_manager

logger = logging.getLogger(__name__)
app = Flask(__name__)

@app.route('/add-export-ledger-to-excel-task/', methods=['POST'])
def add_export_ledger_to_excel_task():
    try:
        data = json.loads(request.data)
    except:
        return {'error_msg': 'invalid data'}, 400
    repo_id = data.get('repo_id')
    parent_dir = data.get('parent_dir')
    try:
        return task_manager.add_export_ledger_to_excel_task(repo_id, parent_dir)
    except Exception as e:
        logger.exception('add ledger export task error: %s', e)
        return {'error_msg': 'Internal Server Error'}, 500


@app.route('/query-task/', methods=['GET'])
def query_task():
    task_id = request.args.get('task_id')
    if not task_id:
        return {'error_msg': 'task_id invalid'}, 400
    return task_manager.query_task(task_id)
