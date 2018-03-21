# coding: utf-8

import os
import logging
import hashlib
import MySQLdb


class ChangeFilePathHandler(object):
    def __init__(self):
        self.db_conn = None
        self.cursor = None

        self.init_seahub_db()

        if self.cursor is None:
            logging.debug('Failed to init seahub db.')

    def init_seahub_db(self):
        try:
            import seahub_settings
            #import local_settings
        except ImportError as e:
            logging.warning('Failed to init seahub db: %s.' %  e)  
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
            #db_infos = local_settings.DATABASES['default']
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

        try:
            # type of msg from ccnet is str, so there need str too,
            # otherwise there will be a conversion operation（ascii is python default encode 
            # type, it's hard to change）.
            self.db_conn = MySQLdb.connect(host=self.db_host, port=self.db_port,
                                           user=self.db_user, passwd=self.db_passwd,
                                           db=self.db_name, charset='utf8', use_unicode=False)
            self.db_conn.autocommit(True)
            self.cursor = self.db_conn.cursor()
        except Exception as e:
            logging.warning('Failed to init seahub db: %s.' %  e)

    def close_seahub_db(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()

    def reconnect_db(self):
        # If connection is timeout, reconnect
        try:
            self.db_conn = MySQLdb.connect(host=self.db_host, port=self.db_port,
                                       user=self.db_user, passwd=self.db_passwd,
                                       db=self.db_name, charset='utf8')
            self.db_conn.autocommit(True)
            self.cursor = self.db_conn.cursor()
        except Exception as e:
            logging.warning('Failed to connect seahub db: %s.' %  e)

    def change_share_file_path(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        if not repo_id or not path or not new_path:
            logging.warning('Failed to change file uuid map, bad args')
            return

        try:
            self._change_share_file_path(repo_id, path, new_path, is_dir, src_repo_id)
        except MySQLdb.OperationalError:
            self.reconnect_db()
            self._change_share_file_path(repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            logging.warning('Failed to change share file path for repo %s, path:%s, new_path: %s, %s.' % (repo_id, path, new_path, e))

    def _change_share_file_path(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        self.cursor.execute('select path from share_fileshare where repo_id=%s and path like %s',
                            [src_repo_id if src_repo_id else repo_id, path + '%'])
        if self.cursor.rowcount == 0:
            return
        # For multi-layer dirs, divide orig_path into orig_parent_path and orig_sub_path
        # new_path_value = new_path + orig_sub_path
        results = self.cursor.fetchall()
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
                self.cursor.execute('''update share_fileshare set repo_id=%s, path=%s
                                where repo_id=%s and path=%s''',
                                (repo_id, new_path_value,
                                src_repo_id if src_repo_id else repo_id, row[0]))

    def change_file_uuid_map(self, repo_id, path, new_path, is_dir, src_repo_id=None):
        if not repo_id or not path or not new_path:
            logging.warning('Failed to change file uuid map, bad args')
            return

        try:
            self._change_file_uuid_map (repo_id, path, new_path, is_dir, src_repo_id)
        except MySQLdb.OperationalError:
            self.reconnect_db()
            self._change_file_uuid_map (repo_id, path, new_path, is_dir, src_repo_id)
        except Exception as e:
            logging.warning('Failed to change file uuid map for repo %s, path:%s, new_path: %s, %s.' % (repo_id, path, new_path, e))

    def _change_file_uuid_map (self, repo_id, path, new_path, is_dir, src_repo_id = None):
        old_dir = os.path.split(path)[0]
        old_file = os.path.split(path)[1]
        self.cursor.execute('select 1 from tags_fileuuidmap where repo_id=%s and parent_path=%s and filename=%s and is_dir=%s',
                            [src_repo_id if src_repo_id else repo_id, old_dir, old_file, is_dir])
        # update old path itself
        if self.cursor.rowcount != 0:
            new_dir = os.path.split(new_path)[0]
            new_file = os.path.split(new_path)[1]

            path_md5 = self.md5_repo_id_parent_path(repo_id, new_dir)

            self.cursor.execute('''update tags_fileuuidmap set repo_id=%s, parent_path=%s, filename=%s,
                            repo_id_parent_path_md5=%s where 
                            repo_id=%s and parent_path=%s and filename=%s and is_dir=%s''',
                            (repo_id, new_dir, new_file, path_md5,
                            src_repo_id if src_repo_id else repo_id, old_dir, old_file, is_dir))

        # sub-dir #
        if is_dir:
            self.cursor.execute ('select parent_path from tags_fileuuidmap where repo_id=%s and parent_path like %s',
                                [src_repo_id if src_repo_id else repo_id, path + '%'])
            if self.cursor.rowcount == 0:
                return
            # For multi-layer dirs, divide orig_path into orig_parent_path and orig_sub_path
            # new_path_value = new_path + orig_sub_path
            results = self.cursor.fetchall()
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

                    self.cursor.execute('''update tags_fileuuidmap set repo_id=%s, parent_path=%s,
                                    repo_id_parent_path_md5=%s where
                                    repo_id=%s and parent_path=%s''',
                                    (repo_id, new_path_value, path_md5,
                                    src_repo_id if src_repo_id else repo_id, row[0]))

    def md5_repo_id_parent_path(self, repo_id, parent_path):
        parent_path = parent_path.rstrip('/') if parent_path != '/' else '/' 
        # repo_id and parent_path are already utf8 encoded characters, can't use encode
        return hashlib.md5(repo_id + parent_path).hexdigest()
