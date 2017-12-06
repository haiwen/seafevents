import logging

from sqlalchemy import func, and_
from sqlalchemy.orm.scoping import scoped_session
from seafobj import CommitDiffer, commit_mgr
from seaserv import seafile_api
from seafevents.db import init_db_session_class
from seafevents.app.config import appconfig
from seafevents.events.models import FileHistory
from seafevents.tasks.file_history import office_version_record_tasks


class RestoreUnrecordHistory(object):
    def __init__(self):
        self._db_session_class = init_db_session_class(appconfig.events_config_file)
        self._history_repo = self.get_exists_history_repo()
        self._current_repo_position = 0
        self._current_commit_position = 0
        self.status = False

    def start(self):
        if not self.status:
            self.status = True
        else:
            logging.info('There is already a task at work.')
        while True:
            try:
                self.do_work()
            except Exception as e:
                logging.error(e)

    def do_work(self):
        # deal one repo at one time.
        repo = seafile_api.get_repo_list(self._current_repo_position, 1)
        if repo.repo_id in [e[0] for e in self._exists_history_repo]:
            commit_id = self.get_repo_last_commit(repo.rpeo_id)
            i, k = 0, 0
            while True:
                temp = [e.id for e in seafile_api.get_commit_list(repo.repo_id, i, 100)]
                if not temp:
                    break
                if commit_id in temp:
                    self._current_commit_position = k * 100 + temp.index 
                    break
                else:
                    k += 1

        while True:
            # add pre-commit task to queue
            try:
                commit = seafile_api.get_commit_list(repo.rpeo_id, self._current_commit_position, 1)
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
                                                         commit, commit_id, repo.repo_id))

            except Exception as e:
                logging.error(e)

        self._current_repo_position += 1
        self._current_commit_position = 0

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

Task = RestoreUnrecordHistory()

if __name__ == '__main__':
    Task.start()
