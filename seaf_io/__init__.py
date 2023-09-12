from threading import Thread

from gevent.pywsgi import WSGIServer

from seafevents.ex_props_handler.ex_props_task_manager import ex_props_task_manager
from seafevents.seaf_io.request_handler import app


class SeafIOServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        ex_props_task_manager.init_config(config)
        self.server = WSGIServer(('127.0.0.1', 6066), app)

    def run(self):
        self.server.serve_forever()
