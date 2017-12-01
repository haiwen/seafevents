import os
import stat
import Queue
import logging
import datetime

from threading import Thread
from seafevents.db import init_db_session_class
from seafevents.events.models import FileHistory
from sqlalchemy.orm.scoping import scoped_session
from seafevents.app.config import appconfig

from seaserv import seafile_api

office_version_record_tasks = Queue.Queue(-1)


class FileHistoryMaster(object):
    def start(self):
        self._db_session_class = init_db_session_class(appconfig.events_config_file)
        for i in xrange(6):
            task_thread = FileHistoryWorker(self._db_session_class)
            task_thread.setDaemon(True)
            task_thread.start()


class FileHistoryWorker(Thread):
    def __init__(self, db_session_class):
        Thread.__init__(self)
        self._db_session_class = db_session_class
        self.files_suffix = ['.'+s for s in appconfig.fh.suffix.split(',')]

    def delete_record(self, session, record, exect=False):
        if record:
            self.d_records.append(record)
        if exect or len(self.d_records) > 100:
            if len(self.d_records) > 0:
                session.query(FileHistory).filter(FileHistory.repo_id == self.d_records[0].repo_id).\
                        filter(FileHistory.path.in_([v.path for v in self.d_records])).delete(synchronize_session='fetch')

                self.d_records = []

    def add_record(self, session, record, exect=False):
        if record:
            self.records.append(record)
        if exect or len(self.records) > 100:
            if len(self.records) > 0:
                try:
                    session.bulk_save_objects(self.records)
                    session.commit()
                except Exception as e:
                    logging.error(e)
                self.records=[]

    def extract_dirs(self, repo_id, dirs, files):
        files = {f.path + f.obj_id: f.__dict__ for f in files}
        while True:
            try:
                d = dirs.pop(0)
            except IndexError:
                return files.values()
            else:
                d_obj_id = d.obj_id if hasattr(d, 'obj_id') else d['obj_id']
                d_path = d.path if hasattr(d, 'path') else d['path']
                d_new_path = d.new_path if hasattr(d, 'new_path') else d['new_path']

                dirents = seafile_api.list_dir_by_dir_id(repo_id, d_obj_id, -1, -1)
                for dirent in dirents:
                    obj = {'path': os.path.join(d_path, dirent.obj_name),
                           'size': dirent.size,
                           'new_path': os.path.join(d_new_path, dirent.obj_name) if d_new_path else '',
                           'obj_id': dirent.obj_id}
                    if stat.S_ISDIR(dirent.mode):
                        dirs.append(obj)
                    else:
                        files[dirent.obj_id] = obj

    def do_work(self, task):
        self.records = []
        self.d_records = []
        session = scoped_session(self._db_session_class)
        try:
            added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
            renamed_files, moved_files, renamed_dirs, moved_dirs, commit, commit_id, repo_id = task

            for de in self.extract_dirs(repo_id, added_dirs, added_files):
                if os.path.splitext(de['path'])[1] in self.files_suffix:
                    time = datetime.datetime.utcfromtimestamp(commit.ctime)
                    self.add_record(session, FileHistory(repo_id, de['path'], commit_id, time, de['obj_id'], de['size'], commit.creator_name))

            for de in modified_files:
                if os.path.splitext(de.path)[1] in self.files_suffix:
                    time = datetime.datetime.utcfromtimestamp(commit.ctime)
                    self.add_record(session, FileHistory(repo_id, de.path, commit_id, time, de.obj_id, de.size, commit.creator_name))

            for de in self.extract_dirs(repo_id, renamed_dirs, renamed_files):
                if os.path.splitext(de['path'])[1] in self.files_suffix:
                    time = datetime.datetime.utcfromtimestamp(commit.ctime)
                    self.add_record(session, FileHistory(repo_id, de['new_path'], commit_id, time, de['obj_id'], de['size'], commit.creator_name, de['path']))

            for de in self.extract_dirs(repo_id, moved_dirs, moved_files):
                if os.path.splitext(de['path'])[1] in self.files_suffix:
                    time = datetime.datetime.utcfromtimestamp(commit.ctime)
                    self.add_record(session, FileHistory(repo_id, de['new_path'], commit_id, time, de['obj_id'], de['size'], commit.creator_name, de['path']))

            for de in self.extract_dirs(repo_id, deleted_dirs, deleted_files):
                if os.path.splitext(de['path'])[1] in self.files_suffix:
                    time = datetime.datetime.utcfromtimestamp(commit.ctime)
                    self.delete_record(session, FileHistory(repo_id, de['path'], commit_id, time, de['obj_id'], de['size'], commit.creator_name, de['new_path']))

            self.add_record(session, None, exect=True)
            self.delete_record(session, None, exect=True)
            session.commit()
        finally:
            session.close()

    def run(self):
        while True:
            task = office_version_record_tasks.get()
            if task:
                self.do_work(task)
