# -*- coding: utf-8 -*-
import logging
from threading import Thread

from werkzeug.serving import ThreadedWSGIServer

from seafevents.compress_service.task_manager import task_manager
from seafevents.compress_service.request_handler import app as application

logger = logging.getLogger(__name__)


class CompressServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)
        task_manager.init(self._workers, self._file_server_port)
        self._server = ThreadedWSGIServer(self._host, int(self._port), application)

    def _parse_config(self, config):
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
        task_manager.run()
        self._server.serve_forever()
