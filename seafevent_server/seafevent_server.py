from threading import Thread

from gevent.pywsgi import WSGIServer
from seafevents.seafevent_server.request_handler import app as application
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seafevents.seasearch.index_task.index_task_manager import index_task_manager
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager


class SeafEventServer(Thread):

    def __init__(self, app, config):
        Thread.__init__(self)
        self._parse_config(config)
        self.app = app
        task_manager.init(self.app, self._workers, self._task_expire_time, config)
        event_export_task_manager.init(self.app, self._workers, self._task_expire_time, config)

        task_manager.run()
        event_export_task_manager.run()
        application.face_recognition_manager = FaceRecognitionManager(config)
        self._server = WSGIServer((self._host, int(self._port)), application)

        index_task_manager.init(config)

    def _parse_config(self, config):
        if config.has_option('SEAF-EVENT-SERVER', 'host'):
            self._host = config.get('SEAF-EVENT-SERVER', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('SEAF-EVENT-SERVER', 'port'):
            self._port = config.getint('SEAF-EVENT-SERVER', 'port')
        else:
            self._port = '8889'

        if config.has_option('SEAF-EVENT-SERVER', 'workers'):
            self._workers = config.getint('SEAF-EVENT-SERVER', 'workers')
        else:
            self._workers = 3

        if config.has_option('SEAF-EVENT-SERVER', 'task_expire_time'):
            self._task_expire_time = config.getint('SEAF-EVENT-SERVER', 'task_expire_time')
        else:
            self._task_expire_time = 30 * 60

    def run(self):
        self._server.serve_forever()
