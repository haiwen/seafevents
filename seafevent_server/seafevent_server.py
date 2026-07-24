from threading import Thread
from waitress import serve

from seafevents.seafevent_server.request_handler import app as application
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seafevents.seafevent_server.import_task_manager import event_import_task_manager
from seafevents.seafevent_server.repo_archive_task_manager import repo_archive_task_manager
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager


class SeafEventServer(Thread):

    def __init__(self, app):
        Thread.__init__(self)
        self._host = '127.0.0.1'
        self._port = 8889
        self._workers = 3
        self._task_expire_time = 30 * 60
        self.app = app
        task_manager.init(self.app, self._workers, self._task_expire_time)
        event_export_task_manager.init(self.app, self._workers, self._task_expire_time)
        event_import_task_manager.init(self.app, self._workers, self._task_expire_time)
        repo_archive_task_manager.init(self.app, self._workers, self._task_expire_time)
        task_manager.run()
        event_export_task_manager.run()
        event_import_task_manager.run()
        repo_archive_task_manager.run()
        application.face_recognition_manager = FaceRecognitionManager()

    def run(self):
        serve(application, host=self._host, port=self._port)
