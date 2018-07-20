import os
import sys
import mock
import uuid
import hashlib
import logging
import argparse
import datetime

from sqlalchemy import func, and_
from sqlalchemy.sql import exists
from sqlalchemy.orm.scoping import scoped_session

from seaserv import seafile_api, get_org_id_by_repo_id
from seafobj import CommitDiffer, commit_mgr
from seafevents.app.config import appconfig, load_config
from seafevents.events.models import FileHistory
from seafevents.events.handlers import generate_filehistory_records, save_records_to_filehistory
from seafevents.events.handlers import should_record


def query_next_record(session, record):
    repo_id_path_md5 = hashlib.md5((record['repo_id'] + record['path']).encode('utf8')).hexdigest()
    q = session.query(FileHistory)
    q = q.filter(FileHistory.repo_id_path_md5 == repo_id_path_md5)
    unchanged_path_record = q.order_by(FileHistory.timestamp).first()
    q = session.query(FileHistory)
    query_key = '%' +  '"old_path": "%s"' % record['path'] + '%'
    changed_path_record = q.filter(FileHistory.repo_id == record['repo_id']).filter(FileHistory.detail.like(query_key)).order_by(FileHistory.timestamp).first()
    if changed_path_record and unchanged_path_record:
        next_record = unchanged_path_record if unchanged_path_record.timestamp < changed_path_record.timestamp else changed_path_record
    elif changed_path_record:
        next_record = changed_path_record
    elif unchanged_path_record:
        next_record = unchanged_path_record
    else:
        next_record = None

    no_another_op = (next_record is None or next_record.op_type != 'recover')
    # what if recover after deleted operation
    if record['op_type'] == 'delete' and no_another_op:
        return None


    return next_record

def save_filehistory(session, record):
    if not should_record(record):
        logging.error('invalid activity record: %s' % record)
        return

    # use same file_uuid if prev item already exists, otherwise new one
    prev_item = query_next_record(session, record)
    if prev_item:
        if record['path'] != prev_item.path and record['op_type'] == 'recover':
            pass
        else:
            record['file_uuid'] = prev_item.file_uuid

    if not record.has_key('file_uuid'):
        file_uuid = uuid.uuid4()
        # avoid hash conflict
        while session.query(exists().where(FileHistory.file_uuid == file_uuid)).scalar():
            file_uuid = uuid.uuid4()
        record['file_uuid'] = file_uuid

    filehistory = FileHistory(record)
    session.add(filehistory)
    session.commit()


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

        load_config(args.config_file)
        self._db_session_class = appconfig.event_session
        self._history_repo = self.get_repo_and_last_time()
        self._current_repo_position = 0
        self._current_commit_position = 0

    def get_repo_last_commits(self, repo_id):
        session = scoped_session(self._db_session_class)
        timestamp = self._history_repo.get(repo_id)
        try:
            res = session.query(FileHistory.commit_id).\
                    filter(and_(FileHistory.repo_id == repo_id, FileHistory.timestamp == timestamp)).all()
            return res
        finally:
            session.close()

    def get_repo_and_last_time(self):
        session = scoped_session(self._db_session_class)
        try:
            res = session.query(FileHistory.repo_id, func.min(FileHistory.timestamp)).group_by(FileHistory.repo_id).all()
            return dict(res)
        finally:
            session.close()

    def start(self):
        while self.do_work() != -1:
            self._current_repo_position += 1

    def diff_and_update(self, repo_id, commit_id, org_id, users):
        # cause some of the properties of the seafile commit object have different names than the seaobj object
        # so take commit from seaobj again
        commit = commit_mgr.load_commit(repo_id, 1, commit_id)
        if commit is None:
            commit = commit_mgr.load_commit(repo_id, 0, commit_id)
        if commit is not None and commit.parent_id and not commit.second_parent_id:
            parent = commit_mgr.load_commit(repo_id, commit.version, commit.parent_id)

            if parent is not None:
                differ = CommitDiffer(repo_id, commit.version, parent.root_id, commit.root_id,
                                      True, True)
                added_files, deleted_files, added_dirs, deleted_dirs, modified_files,\
                        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff_to_unicode()

                time = datetime.datetime.utcfromtimestamp(commit.ctime)
                session = scoped_session(self._db_session_class)

                if added_files or deleted_files or added_dirs or deleted_dirs or \
                        modified_files or renamed_files or moved_files or renamed_dirs or moved_dirs:
                    records = generate_filehistory_records(added_files, deleted_files,
                            added_dirs, deleted_dirs, modified_files, renamed_files,
                            moved_files, renamed_dirs, moved_dirs, commit, repo_id,
                            parent, time)

                    with mock.patch('seafevents.events.handlers.save_filehistory', side_effect=save_filehistory):
                        if appconfig.fh.enabled:
                            save_records_to_filehistory(session, records)

                session.close()
                self._current_commit_position += 1

    def do_work(self):
        self._current_commit_position = 0
        repo = seafile_api.get_repo_list(self._current_repo_position, 1)
        if not repo:
            return -1
        repo = repo[0]
        logging.info('Start processing repo :%s', repo.repo_id)

        org_id = get_org_id_by_repo_id(repo.repo_id)
        repo_id = repo.repo_id
        if org_id > 0:
            users_obj = seafile_api.org_get_shared_users_by_repo(org_id, repo_id)
            owner = seafile_api.get_org_repo_owner(repo_id)
        else:
            users_obj = seafile_api.get_shared_users_by_repo(repo_id)
            owner = seafile_api.get_repo_owner(repo_id)
        users = [e.user for e in users_obj] + [owner]

        self._last_commit_id = None
        if repo_id in self._history_repo.keys():
            commit_ids = self.get_repo_last_commits(repo_id)
            count = 0
            k = 0
            bk = False
            while True:
                temp = [e.id for e in seafile_api.get_commit_list(repo_id, k * 100, 100)]
                if not temp:
                    break
                # avoid two commit at the same time
                for commit_id in commit_ids:
                    if commit_id[0] in temp:
                        count += 1

                    if count == len(commit_ids):
                        self._current_commit_position = k * 100 + temp.index(commit_id[0]) + 1
                        self._last_commit_id = commit_id[0]
                        bk = True
                        break
                if bk:
                    break
                k += 1
        else:
            # keeping _current_commit_position zero will restore all activity records of the repo
            commit_objs = seafile_api.get_commit_list(repo_id, self._current_commit_position, 1)
            current_commit_id = [e.id for e in commit_objs][0]
            self._last_commit_id = current_commit_id
            self.diff_and_update(repo_id, current_commit_id, org_id, users)

        start_commit_position = self._current_commit_position
        count_offest = 0
        while True:
            # get last commit and another commits
            # avoid current_commit_position expired by generate new record
            commit_objs = seafile_api.get_commit_list(repo_id, self._current_commit_position - 1, 5)
            commit_ids = [e.id for e in commit_objs]

            if not commit_objs or len(commit_objs) == 1:
                break

            if self._last_commit_id not in commit_ids or commit_objs[-1].id == self._last_commit_id:
                self._current_commit_position += 4
                count_offest = 4
            else:
                offset = commit_ids.index(self._last_commit_id)
                self._current_commit_position += offset
                current_commit_id = commit_ids[offset + 1]
                self._last_commit_id = commit_ids[offset + 1]
                self.diff_and_update(repo_id, current_commit_id, org_id, users)
                count_offest = 1

        count = self._current_commit_position - start_commit_position - count_offest
        logging.info("%s recover %s activity records" % (repo_id, count))


if __name__ == '__main__':
    task = RestoreUnrecordHistory()
    task.start()
