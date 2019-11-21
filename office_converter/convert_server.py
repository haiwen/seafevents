import os
import re
import socket
import argparse
import logging
import json
from urllib import parse
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler

import jwt

from seafevents.office_converter.task_manager import task_manager
from seafevents.office_converter.doctypes import DOC_TYPES, PPT_TYPES, EXCEL_TYPES

logger = logging.getLogger(__name__)
FILE_ID_PATTERN = re.compile(r'^[0-9a-f]{40}$')
supported_doctypes = DOC_TYPES + PPT_TYPES + EXCEL_TYPES + ('pdf', )


def _valid_file_id(file_id):
    if not isinstance(file_id, str):
        return False
    return FILE_ID_PATTERN.match(str(file_id)) is not None


def _check_type_and_file_id(file_id, doctype):
    if doctype not in supported_doctypes:
        logger.error('doctype "%s" is not supported' % doctype)
        raise Exception('doctype "%s" is not supported' % doctype)

    if not _valid_file_id(file_id):
        logger.error('invalid file id')
        raise Exception('invalid file id')


class ConverterRequestHandler(SimpleHTTPRequestHandler):

    def do_GET(self):
        auth = self.headers['Authorization'].split()
        if not auth or auth[0].lower() != 'token' or len(auth) != 2:
            self.send_error(403, 'Token invalid.')
        token = auth[1]
        if not token:
            self.send_error(403, 'Token invalid.')
        try:
            jwt.decode(token, 'secret', algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            self.send_error(403, 'Token expired.')

        path, args = parse.splitquery(self.path)
        args = parse.parse_qs(args)
        if path == '/add-task':
            try:
                _check_type_and_file_id(args['file_id'][0], args['doctype'][0])
                task_manager.add_task(args['file_id'][0], args['doctype'][0], args['raw_path'][0])
            except Exception as e:
                self.send_error(500, e)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = 'OK'.encode('UTF-8', 'replace')
            self.wfile.write(resp)

        elif path == '/query-status':
            try:
                _valid_file_id(args['file_id'][0])
                resp = task_manager.query_task_status(args['file_id'][0], args['doctype'][0])
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode())
            except Exception as e:
                self.send_error(500, e)

        else:
            self.send_error(400, 'path %s invalid.' % path)


def main():
    # parse argument
    parser = argparse.ArgumentParser()
    parser.add_argument('--outputdir', help='office converter host')
    parser.add_argument('--workers', help='office converter host')
    parser.add_argument('--max_pages', help='office converter host')
    parser.add_argument('--host', help='office converter host')
    parser.add_argument('--port', help='office converter port')
    args = parser.parse_args()

    # start task_manager
    task_manager.init(
        num_workers=int(args.workers),
        max_pages=int(args.max_pages),
        pdf_dir=os.path.join(args.outputdir, 'pdf'),
        html_dir=os.path.join(args.outputdir, 'html')
    )
    task_manager.run()

    # start http server
    server_address = (args.host, int(args.port))
    print("[%s] Start converter on %s:%s" % (datetime.now(), args.host, int(args.port)))

    # test if the http server has already started
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ret_code = sock.connect_ex((args.host, int(args.port)))
    if ret_code == 0:
        print("[%s] Existing converter http server found, on %s:%s." %
              (datetime.now(), args.host, int(args.port)))
    else:
        convert_server = HTTPServer(server_address, ConverterRequestHandler)
        print("[%s] Start converter http server." % datetime.now())
        convert_server.serve_forever()

    sock.close()


if __name__ == '__main__':
    main()
