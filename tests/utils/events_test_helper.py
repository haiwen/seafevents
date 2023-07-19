# -*- coding: utf-8 -*-
import os
import uuid
import hashlib
import datetime

from sqlalchemy.sql import text, exists, select

from seafevents.events.db import query_prev_record, update_file_history_record
from seafevents.events.models import FileHistory
from seafevents.tests.conftest import get_db_session


def save_file_history(session, record):
    prev_item = query_prev_record(session, record)
    if prev_item:
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(minutes=1)
        if record['op_type'] == 'edit' and prev_item.op_type == 'edit' \
                and prev_item.op_user == record['op_user'] \
                and prev_item.timestamp > dt - delta:
            update_file_history_record(session, prev_item.id, record)
            return

        if record['path'] != prev_item.path and record['op_type'] == 'recover':
            pass
        else:
            record['file_uuid'] = prev_item.file_uuid

    if 'file_uuid' not in record:
        file_uuid = uuid.uuid4().__str__()
        while session.scalar(select(exists().where(FileHistory.file_uuid == file_uuid))):
            file_uuid = uuid.uuid4().__str__()
        record['file_uuid'] = file_uuid

    file_history = FileHistory(record)
    session.add(file_history)
    session.commit()


class ChangeFilePathHandler(object):
    def __init__(self):
        self.session = get_db_session('TESTDB')

    def close_session(self):
        self.session.close()

    def update_db_records(self, dst_repo_id, path, new_path, is_dir, src_repo_id=None):
        if not dst_repo_id or not path or not new_path:
            return

        self.change_file_uuid_map(dst_repo_id, path, new_path, is_dir, src_repo_id)
        self.change_share_file_path(dst_repo_id, path, new_path, src_repo_id)
        self.change_upload_share_file_path(dst_repo_id, path, new_path, src_repo_id)

    def change_share_file_path(self, repo_id, path, new_path, src_repo_id=None):
        try:
            self._change_share_file_path(repo_id, path, new_path, src_repo_id)
        except Exception as e:
            print(e)

    def _change_share_file_path(self, repo_id, path, new_path, src_repo_id=None):
        result = self.session.execute(
            text('select path from share_fileshare where repo_id=:repo_id and path like :path'),
            {'repo_id': src_repo_id if src_repo_id else repo_id, 'path': path + '%'}
        )
        if result.rowcount == 0:
            return

        results = result.fetchall()
        for row in results:
            if row[0] == path or row[0].startswith(path + '/'):
                if row[0] == path:
                    new_path_value = new_path
                else:
                    new_path_value = new_path + row[0].split(path, 1)[1]

                self.session.execute(
                    text('update share_fileshare set repo_id=:new_repo_id, path=:new_path '
                         'where repo_id=:old_repo_id and path=:old_path'),
                    {'new_repo_id': repo_id, 'new_path': new_path_value,
                     'old_repo_id': src_repo_id if src_repo_id else repo_id, 'old_path': row[0]}
                )
                self.session.commit()

    def change_file_uuid_map(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        try:
            self._change_file_uuid_map(repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            print(e)

    def _change_file_uuid_map(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        old_dir = os.path.split(path)[0]
        old_file = os.path.split(path)[1]
        result = self.session.execute(
            text('select 1 from tags_fileuuidmap where repo_id=:repo_id and '
                 'parent_path=:parent_path and filename=:filename and is_dir=:is_dir'),
            {'repo_id': src_repo_id if src_repo_id else repo_id, 'parent_path': old_dir,
             'filename': old_file, 'is_dir': is_dir}
        )

        if result.rowcount != 0:
            new_dir = os.path.split(new_path)[0]
            new_file = os.path.split(new_path)[1]
            path_md5 = self.md5_repo_id_parent_path(repo_id, new_dir)

            self.session.execute(
                text('update tags_fileuuidmap set repo_id=:new_repo_id, parent_path=:new_dir, '
                     'filename=:new_file, repo_id_parent_path_md5=:new_md5 where repo_id=:repo_id '
                     'and parent_path=:dir and filename=:filename and is_dir=:is_dir'),
                {'new_repo_id': repo_id, 'new_dir': new_dir, 'new_file': new_file, 'new_md5': path_md5, 'repo_id':
                    src_repo_id if src_repo_id else repo_id, 'dir': old_dir, 'filename': old_file, 'is_dir': is_dir}
            )
            self.session.commit()

        if is_dir:
            result = self.session.execute(
                text('select parent_path from tags_fileuuidmap where repo_id=:repo_id and parent_path like :path'),
                {'repo_id': src_repo_id if src_repo_id else repo_id, 'path': path + '%'})
            if result.rowcount == 0:
                return

            results = result.fetchall()
            for row in results:
                if row[0] == path or row[0].startswith(path + '/'):
                    if row[0] == path:
                        new_path_value = new_path
                    else:
                        new_path_value = new_path + row[0].split(path, 1)[1]
                    path_md5 = self.md5_repo_id_parent_path(repo_id, new_path_value)

                    self.session.execute(
                        text('update tags_fileuuidmap set repo_id=:new_repo_id, parent_path=:new_dir, '
                             'repo_id_parent_path_md5=:new_md5 where repo_id=:repo_id and parent_path=:dir'),
                        {'new_repo_id': repo_id, 'new_dir': new_path_value, 'new_md5': path_md5,
                         'repo_id': src_repo_id if src_repo_id else repo_id, 'dir': row[0]}
                    )
                    self.session.commit()

    @staticmethod
    def md5_repo_id_parent_path(repo_id, parent_path):
        parent_path = parent_path.rstrip('/') if parent_path != '/' else '/'
        return hashlib.md5((repo_id + parent_path).encode('utf-8')).hexdigest()

    def change_upload_share_file_path(self, repo_id, path, new_path, src_repo_id=None):
        try:
            self._change_upload_share_file_path(repo_id, path, new_path, src_repo_id)
        except Exception as e:
            print(e)

    def _change_upload_share_file_path(self, repo_id, path, new_path, src_repo_id):
        result = self.session.execute(
            text('select path from share_uploadlinkshare where repo_id=:repo_id and path like :dir'),
            {'repo_id': src_repo_id if src_repo_id else repo_id, 'dir': path + '%'}
        )

        if result.rowcount == 0:
            return

        results = result.fetchall()
        for row in results:
            if row[0] == path or row[0].startswith(path + '/'):
                if row[0] == path:
                    new_path_value = new_path
                else:
                    new_path_value = new_path + row[0].split(path, 1)[1]

                self.session.execute(
                    text('update share_uploadlinkshare set repo_id=:new_repo_id, path=:new_path'
                         ' where repo_id=:repo_id and path=:path'),
                    {'new_repo_id': repo_id, 'new_path': new_path_value,
                     'repo_id': src_repo_id if src_repo_id else repo_id, 'path': row[0]}
                )
                self.session.commit()
