from threading import Thread

from gevent.pywsgi import WSGIServer

from seafevents.seaf_io.request_handler import app as application
from seafevents.seaf_io.task_manager import task_manager

class SeafeventsIOServer(Thread):

    def __init__(self):
        Thread.__init__(self)
        self._server = WSGIServer(('127.0.0.1', 6000), application)

    def run(self):
        task_manager.start()
        self._server.serve_forever()
