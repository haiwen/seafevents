# -*- coding: utf-8 -*-
import logging

from flask import Flask, request, make_response

from seafevents.compress_service.task_manager import task_manager

app = Flask(__name__)
logger = logging.getLogger(__name__)


@app.route('/add-compress-task', methods=['GET'])
def add_compress_task():
    token = request.args.get('token')
    repo_id = request.args.get('repo_id')
    file_path = request.args.get('file_path')
    last_modify = request.args.get('last_modify')
    decrypted_pwd = request.args.get('decrypted_pwd')

    try:
        resp, error = task_manager.add_compress_task(token, repo_id, file_path, last_modify, decrypted_pwd)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    if resp:
        logger.debug('Add compress task succeed.')
        return make_response(({'success': True}, 200))
    else:
        return make_response((error, 500))


@app.route('/query-compress-status', methods=['GET'])
def query_compress_status():
    token = request.args.get('token')
    repo_id = request.args.get('repo_id')
    file_path = request.args.get('file_path')
    last_modify = request.args.get('last_modify')

    try:
        task_status = task_manager.query_compress_status(token, repo_id, file_path, last_modify)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_status': task_status}, 200))
