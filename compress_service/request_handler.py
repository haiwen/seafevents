# -*- coding: utf-8 -*-
import json
import urllib
import urlparse
import logging
from SimpleHTTPServer import SimpleHTTPRequestHandler

from seafevents.compress_service.task_manager import task_manager


logger = logging.getLogger(__name__)


class CompressRequestHandler(SimpleHTTPRequestHandler):

    def do_GET(self):
        path, arguments = urllib.splitquery(self.path)
        arguments = urlparse.parse_qs(arguments)
        if path == '/add-compress-task':
            token = arguments['token'][0]
            repo_id = arguments['repo_id'][0]
            file_path = arguments['file_path'][0]
            last_modify = arguments['last_modify'][0]
            decrypted_pwd = arguments['decrypted_pwd'][0]

            try:
                resp, error = task_manager.add_compress_task(token, repo_id, file_path, last_modify, decrypted_pwd)
            except Exception as e:
                logger.error(e)
                self.send_error(500, e)
                return

            if resp:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                resp = 'OK'.encode('UTF-8', 'replace')
                self.wfile.write(resp)
            else:
                self.send_error(500, error)
                return

        if path == '/query-compress-status':
            token = arguments['token'][0]
            repo_id = arguments['repo_id'][0]
            file_path = arguments['file_path'][0]
            last_modify = arguments['last_modify'][0]

            try:
                task_status = task_manager.query_compress_status(token, repo_id, file_path, last_modify)
            except Exception as e:
                logger.error(e)
                self.send_error(500, e)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_status': task_status}
            self.wfile.write(json.dumps(resp).encode('utf-8'))
