import os
import sys
import time
import logging
import argparse

from sqlalchemy import func, and_
from sqlalchemy.orm.scoping import scoped_session
from seafobj import CommitDiffer, commit_mgr
from seaserv import seafile_api
from seafevents.db import init_db_session_class
from seafevents.app.app import App
from seafevents.app.config import appconfig
from seafevents.events.models import FileHistory
from seafevents.tasks.file_history import office_version_record_tasks, FileHistoryMaster


class RestoreUnrecordHistory(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='seafevents main program'
        )
        self._parser.add_argument(
            '--config-file',
            default=os.path.join(os.getcwd(), 'events.conf'),
            help='seafevents config file')
        args = self._parser.parse_args()
        kw = {
            'format': '[%(asctime)s] [%(levelname)s] %(message)s',
            'datefmt': '%m/%d/%Y %H:%M:%S',
            'level': 0,
            'stream': sys.stdout
        }
        logging.basicConfig(**kw)
        # basicconfig didn't work
        logging.getLogger().setLevel(logging.INFO)



        App.load_config(appconfig, args.config_file)
        self._db_session_class = init_db_session_class(appconfig.events_config_file)
        self._history_repo = self._get_last_repo_and_commit()
        self._current_repo_position = 0
        self._current_commit_position = 0
        self.DEL_CODE = 'dddddddddddddddddddddddddddddd'

    def start(self):
        filehistory_master = FileHistoryMaster()
        filehistory_master.start(deleted=False)

        while True:
            try:
                if self.do_work() == -1:
                    break
            except Exception as e:
                logging.error(e)

    def do_work(self):
        # deal one repo at one time.
        repo = seafile_api.get_repo_list(self._current_repo_position, 1)
        if not repo:
            return -1
        repo = repo[0]
        logging.info('Start processing repo :%s', repo.repo_id)
        if repo.repo_id in self._history_repo.keys():
            commit_id = self.get_repo_last_commit(repo.repo_id)
            k = 0
            while True:
                temp = [e.id for e in seafile_api.get_commit_list(repo.repo_id, k*100, 100)]
                if not temp:
                    break
                if commit_id[0] in temp:
                    self._current_commit_position = k * 100 + temp.index(commit_id[0]) + 1
                    break
                else:
                    k += 1

        while True:
            if office_version_record_tasks.qsize() > 2000:
                time.sleep(1)
                continue
            try:
            # add pre-commit task to queue
                commit = seafile_api.get_commit_list(repo.repo_id, self._current_commit_position, 1)
                if not commit:
                    break
                commit = commit[0]
                if commit is not None and commit.parent_id and not commit.second_parent_id:
                    parent = commit_mgr.load_commit(repo.repo_id, commit.version, commit.parent_id)
                    if parent is not None:
                        differ = CommitDiffer(repo.repo_id, commit.version, parent.root_id, commit.root_id,
                                              True, True)
                        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
                        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

                        office_version_record_tasks.put((added_files, deleted_files, added_dirs,
                                                         deleted_dirs, modified_files, renamed_files,
                                                         moved_files, renamed_dirs, moved_dirs, 
                                                         commit, commit.id, repo.repo_id))

            except Exception as e:
                logging.error(e)
            finally:
                self._current_commit_position += 1

        self.clear_repo(repo.repo_id)
        logging.info('repo :%s has been processed', repo.repo_id)
        self._current_repo_position += 1
        self._current_commit_position = 0

    def clear_repo(self, repo_id):
        session = scoped_session(self._db_session_class)
        while True:
            e = session.query(FileHistory).filter(
                and_(FileHistory.repo_id == repo_id,
                FileHistory.file_id == self.DEL_CODE,
                FileHistory.commit_id == self.DEL_CODE)
            ).first()
            if not e:
                break
            session.query(FileHistory).filter(and_(FileHistory.repo_id == e.repo_id,
                                                   FileHistory.path == e.path),
                                                   FileHistory.ctime <= e.ctime).delete(synchronize_session='fetch')

    def get_repo_last_commit(self, repo_id):
        session = scoped_session(self._db_session_class)
        ctime = self._history_repo.get(repo_id)
        try:
            res = session.query(FileHistory.commit_id).filter(and_(FileHistory.repo_id == repo_id, FileHistory.ctime == ctime)).first()
            return res
        finally:
            session.close()

    def _get_last_repo_and_commit(self):
        session = scoped_session(self._db_session_class)
        try:
            res = session.query(FileHistory.repo_id, func.min(FileHistory.ctime)).group_by(FileHistory.repo_id).all()
            return dict(res)
        finally:
            session.close()


if __name__ == '__main__':
    task = RestoreUnrecordHistory()
    task.start()
