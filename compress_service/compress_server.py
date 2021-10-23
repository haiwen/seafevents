# -*- coding: utf-8 -*-
import time
import logging
from threading import Thread
from BaseHTTPServer import HTTPServer

from seafevents.compress_service.task_manager import task_manager
from seafevents.compress_service.request_handler import CompressRequestHandler


logger = logging.getLogger(__name__)


class CompressServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)
        task_manager.init(self._workers, self._file_server_port)
        task_manager.run()
        self._server = HTTPServer((self._host, int(self._port)), CompressRequestHandler)

    def is_server_enabled(self):
        return self.server_enabled

    def is_worker_enabled(self):
        return self.worker_enabled

    def _parse_config(self, config):
        if config.has_option('COMPRESS SERVER', 'server_enabled'):
            self.server_enabled = config.getboolean('COMPRESS SERVER', 'server_enabled')
        else:
            self.server_enabled = False

        if config.has_option('COMPRESS SERVER', 'worker_enabled'):
            self.worker_enabled = config.getboolean('COMPRESS SERVER', 'worker_enabled')
        else:
            self.worker_enabled = False

        if config.has_option('COMPRESS SERVER', 'host'):
            self._host = config.get('COMPRESS SERVER', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('COMPRESS SERVER', 'port'):
            self._port = config.getint('COMPRESS SERVER', 'port')
        else:
            self._port = 1000

        if config.has_option('COMPRESS SERVER', 'workers'):
            self._workers = config.getint('COMPRESS SERVER', 'workers')
        else:
            self._workers = 4

        if config.has_option('COMPRESS SERVER', 'file_server_port'):
            self._file_server_port = config.getint('COMPRESS SERVER', 'file_server_port')
        else:
            self._file_server_port = 8082

    def run(self):
        while 1:
            try:
                self._server.serve_forever()
            except Exception as e:
                logger.error(e)
                time.sleep(5)
                self._server.server_close()
                self._server = HTTPServer((self._host, int(self._port)), CompressRequestHandler)
