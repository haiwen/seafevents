from threading import Thread
from gevent.pywsgi import WSGIServer

from seafevents.file_converter.apis import flask_app


class ConverterServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)

        self._server = WSGIServer((self._host, int(self._port)), flask_app)

    def _parse_config(self, config):
        if config.has_option('FILE CONVERTER', 'host'):
            self._host = config.get('FILE CONVERTER', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('FILE CONVERTER', 'port'):
            self._port = config.getint('FILE CONVERTER', 'port')
        else:
            self._port = '8888'

    def run(self):
        self._server.serve_forever()
