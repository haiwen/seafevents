# coding: utf-8

import os
import logging
import hashlib

from sqlalchemy.sql import text
from seafevents.db import create_mysql_session
from sqlalchemy.orm.scoping import scoped_session


class ChangeFilePathHandler(object):
    def __init__(self):
        self.db_conn = None

        self.init_seahub_db()

    def init_seahub_db(self):
        try:
            import seahub_settings
            # import local_settings
        except ImportError as e:
            logging.warning('Failed to init seahub db: %s.' %  e)
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
            # db_infos = local_settings.DATABASES['default']
        except KeyError as e:
            logging.warning('Failed to init seahub db, can not find db info in seahub settings.')
            return

        if db_infos.get('ENGINE') != 'django.db.backends.mysql':
            logging.warning('Failed to init seahub db, only mysql db supported.')
            return

        self.db_host = db_infos.get('HOST', '127.0.0.1')
        self.db_port = int(db_infos.get('PORT', '3306'))
        self.db_name = db_infos.get('NAME')
        if not self.db_name:
            logging.warning('Failed to init seahub db, db name is not setted.')
            return
        self.db_user = db_infos.get('USER')
        if not self.db_user:
            logging.warning('Failed to init seahub db, db user is not setted.')
            return
        self.db_passwd = db_infos.get('PASSWORD')

        self.MysqlSession = create_mysql_session(self.db_host, self.db_port,
                                           self.db_user, self.db_passwd, self.db_name)
        self.Session = scoped_session(self.MysqlSession)

    def trans_to_unicode(self, str_list):
        return [ e.decode('utf-8') for e in str_list]


    def update_db_records(self, dst_repo_id, path, new_path, is_dir, src_repo_id=None):
        if not dst_repo_id or not path or not new_path:
            logging.warning('Failed to change file uuid map, bad args')
            return

        dst_repo_id, path, new_path = self.trans_to_unicode([dst_repo_id, path, new_path])
        if src_repo_id:
            src_repo_id = self.trans_to_unicode([src_repo_id])
        self.change_file_uuid_map(dst_repo_id, path, new_path, is_dir, src_repo_id)
        self.change_share_file_path(dst_repo_id, path, new_path, is_dir, src_repo_id)
        self.change_upload_share_file_path(dst_repo_id, path, new_path, is_dir, src_repo_id)

    def change_share_file_path(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        try:
            thread_local_session = self.Session()
            self._change_share_file_path(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            thread_local_session = self.Session()
            self._change_share_file_path(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            logging.warning('Failed to change share file path for repo %s, path:%s, new_path: %s, %s.' % (repo_id, path, new_path, e))
        finally:
            # The scoped_session.remove() method, as always, removes the current Session associated with the thread, if any.
            self.Session.remove()

    def _change_share_file_path(self, session, repo_id, path, new_path, is_dir, src_repo_id=None):
        result = session.execute(text('select path from share_fileshare where repo_id=:repo_id and path like :path')
                                 , {'repo_id': src_repo_id if src_repo_id else repo_id, 'path': path + '%'})
        if result.rowcount == 0:
            return

        # For multi-layer dirs, divide orig_path into orig_parent_path and orig_sub_path
        # new_path_value = new_path + orig_sub_path
        results = result.fetchall()
        # get all records that path starts with old path
        # e.g
        # old_path: /old_path/t
        # get all results: /old_path/t /old_path/t1  /old_path/t/q
        for row in results:
            # row[0]: old path in db
            # path: old path
            # new_path_value: new path

            # pass only oldpath and subdir path
            if row[0] == path or row[0].startswith(path + '/'):
                if row[0] == path:
                    new_path_value = new_path
                else:
                    new_path_value = new_path + row[0].split(path, 1)[1]

                # update old path and subdir record
                session.execute(text('update share_fileshare set repo_id=:new_repo_id, path=:new_path '
                                     'where repo_id=:old_repo_id and path=:old_path'),
                                {'new_repo_id': repo_id, 'new_path': new_path_value,
                                 'old_repo_id': src_repo_id if src_repo_id else repo_id, 'old_path': row[0]})
                session.commit()

    def change_file_uuid_map(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        try:
            thread_local_session = self.Session()
            self._change_file_uuid_map(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception:
            thread_local_session = self.Session()
            self._change_file_uuid_map(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            logging.warning('Failed to change file uuid map for repo %s, path:%s, new_path: %s, %s.' % (repo_id, path, new_path, e))
        finally:
            self.Session.remove()

    def _change_file_uuid_map(self, session, repo_id, path, new_path, is_dir, src_repo_id = None):
        old_dir = os.path.split(path)[0]
        old_file = os.path.split(path)[1]
        result = session.execute(text('select 1 from tags_fileuuidmap where repo_id=:repo_id and parent_path=:parent_path and filename=:filename and is_dir=:is_dir'),
                                 {'repo_id': src_repo_id if src_repo_id else repo_id, 'parent_path': old_dir, 'filename': old_file, 'is_dir': is_dir})
        # update old path itself
        if result.rowcount != 0:
            new_dir = os.path.split(new_path)[0]
            new_file = os.path.split(new_path)[1]

            path_md5 = self.md5_repo_id_parent_path(repo_id, new_dir)

            session.execute(text('update tags_fileuuidmap set repo_id=:new_repo_id, parent_path=:new_dir, filename=:new_file, '
                                 'repo_id_parent_path_md5=:new_md5 where '
                                 'repo_id=:repo_id and parent_path=:dir and filename=:filename and is_dir=:is_dir'),
                            {'new_repo_id': repo_id, 'new_dir': new_dir, 'new_file': new_file, 'new_md5': path_md5,
                             'repo_id': src_repo_id if src_repo_id else repo_id, 'dir': old_dir, 'filename': old_file, 'is_dir': is_dir})
            session.commit()

        # sub-dir #
        if is_dir:
            result = session.execute(text('select parent_path from tags_fileuuidmap where repo_id=:repo_id and parent_path like :path'),
                                     {'repo_id':src_repo_id if src_repo_id else repo_id, 'path': path + '%'})
            if result.rowcount == 0:
                return
            # For multi-layer dirs, divide orig_path into orig_parent_path and orig_sub_path
            # new_path_value = new_path + orig_sub_path
            results = result.fetchall()
            # get all record that parent_path starts with old parent_path
            # e.g
            # old_path: /old_path/t
            # get all resultts: /old_path/t /old_path1/t  /olt_path/t/1
            for row in results:
                # row[0]: old parent_path in db
                # path: old  path
                # new_path_value: new path

                # pass only oldpath and subdir path
                if row[0] == path or row[0].startswith(path + '/'):
                    if row[0] == path:
                        new_path_value = new_path
                    else:
                        new_path_value = new_path + row[0].split(path, 1)[1]
                    path_md5 = self.md5_repo_id_parent_path(repo_id, new_path_value)

                    session.execute(text('update tags_fileuuidmap set repo_id=:new_repo_id, parent_path=:new_dir, '
                                         'repo_id_parent_path_md5=:new_md5 where '
                                         'repo_id=:repo_id and parent_path=:dir'),
                                    {'new_repo_id': repo_id, 'new_dir': new_path_value, 'new_md5': path_md5,
                                     'repo_id': src_repo_id if src_repo_id else repo_id, 'dir': row[0]})
                    session.commit()

    def md5_repo_id_parent_path(self, repo_id, parent_path):
        parent_path = parent_path.rstrip('/') if parent_path != '/' else '/'
        return hashlib.md5((repo_id + parent_path).encode('utf-8')).hexdigest().decode('utf-8')

    def change_upload_share_file_path(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        try:
            thread_local_session = self.Session()
            self._change_upload_share_file_path(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception:
            thread_local_session = self.Session()
            self._change_upload_share_file_path(thread_local_session, repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            logging.warning('Failed to change upload share file path for repo %s, path:%s, new_path: %s, %s.' % (repo_id, path, new_path, e))
        finally:
            self.Session.remove()

    def _change_upload_share_file_path(self, session, repo_id, path, new_path, is_dir, src_repo_id):
        result = session.execute(text('select path from share_uploadlinkshare where repo_id=:repo_id and path like :dir'),
                                {'repo_id': src_repo_id if src_repo_id else repo_id, 'dir': path + '%'})

        if result.rowcount == 0:
            return
        # For multi-layer dirs, divide orig_path into orig_parent_path and orig_sub_path
        # new_path_value = new_path + orig_sub_path
        results = result.fetchall()
        # get all records that path starts with old path
        # e.g
        # old_path: /old_path/t
        # get all results: /old_path/t /old_path/t1  /old_path/t/q
        for row in results:
            # row[0]: old path in db
            # path: old path
            # new_path_value: new path

            # pass only oldpath and subdir path
            if row[0] == path or row[0].startswith(path + '/'):
                if row[0] == path:
                    new_path_value = new_path
                else:
                    new_path_value = new_path + row[0].split(path, 1)[1]

            # update old path and subdir record
            session.execute(text('update share_uploadlinkshare set repo_id=:new_repo_id, path=:new_path '
                                 'where repo_id=:repo_id and path=:path'),
                            {'new_repo_id': repo_id, 'new_path': new_path_value,
                             'repo_id': src_repo_id if src_repo_id else repo_id, 'path': row[0]})
            session.commit()
