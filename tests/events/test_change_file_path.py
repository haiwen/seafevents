# coding:utf8

import os
import sys
import pytest

from seafevents.tests.utils import EventTest, ChangeFilePathHandler
from seafevents.tests.conftest import read_db_conf


class seahub_settings():
    host, port, username, passwd, dbname = read_db_conf('TESTDB')
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
            'NAME': dbname,  # Or path to database file if using sqlite3.
            'USER': username,                      # Not used with sqlite3.
            'PASSWORD': passwd,                  # Not used with sqlite3.
            'HOST': host,                      # Set to empty string for localhost. Not used with sqlite3.
            'PORT': port,                      # Set to empty string for default. Not used with sqlite3.
        }
    }
sys.modules['seahub_settings'] = seahub_settings
changer = ChangeFilePathHandler()

@pytest.mark.usefixtures("test_db")
class ChangeFilePathTest(EventTest):
    def setUp(self):
        sql = "insert into share_uploadlinkshare(username, repo_id, path, token, ctime, view_cnt) values" +  \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/', '6933639a376f42afa7ab', '2017-11-08 05:28:50.554830', 0)," + \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/test/', 'ab045b07dca5449d8752', '2017-11-08 05:28:50.554830', 0)," + \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/编码/', 'ab045b07dca5449d8711', '2017-11-08 05:28:50.554830', 0)," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_1/test/', '55d184bd303e458cac80', '2017-11-08 05:28:50.554830', 0)," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_1/中文/', '55d184bd303e458cac12', '2017-11-08 05:28:50.554830', 0)," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_backup/', '33f5446a3ded4c5ba5c2', '2017-11-08 05:28:50.554830', 0)"
        self.exec_sql('TESTDB', sql, {})
        sql = "insert into share_fileshare(username, repo_id, path, token, ctime, view_cnt, s_type, permission) values" +  \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/', '6933639a376f42afa71b', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," + \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/test/', 'ab045b07dca5449d8722', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," + \
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/编码/', 'ab045b07dca5449d8731', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_1/test/', '55d184bd303e458cac40', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_1/中文/', '55d184bd303e458cac52', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures_backup/', '33f5446a3ded4c5ba562', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/files/girl.jpg', '33f5446a3ded4c5ba571', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/files/gir.jpg', '33f5446a3ded4c5ba583', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/files/girls.jpg', '33f5446a3ded4c5ba594', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')," +\
              "('admin@admin.com', 'c89409c5-b52c-4469-91ba-b222a5d3efff', '/files/girls.jpg1', '33f5446a3ded4c5ba505', '2017-11-08 05:28:50.554830', 0, 'd', 'view_download')"
        self.exec_sql('TESTDB', sql, {})
        sql = "insert into tags_fileuuidmap(uuid, repo_id, repo_id_parent_path_md5, parent_path, filename, is_dir) values" +  \
              "('26c313b0feb640a29a0606ac9cc99003', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b3d', '/Pictures', '格莱美.jpg', 0)," + \
              "('26c313b0feb640a29a0606ac9cc99001', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b31', '/Pictures/编码', 'test.jpg', 0)," + \
              "('26c313b0feb640a29a0606ac9cc99002', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b32', '/Picturesfff', 'test.jpg', 0)," + \
              "('26c313b0feb640a29a0606ac9cc99004', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b33', '/Pictures编码', 'test.jpg', 0)," + \
              "('26c313b0feb640a29a0606ac9cc99005', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b34', '/Pictures', '编码.jpg', 0)," + \
              "('26c313b0feb640a29a0606ac9cc99006', 'c89409c5-b52c-4469-91ba-b222a5d3efff', 'b3cdec4cd2d85605a1a219047dd80b35', '/Picture', 'test.jpg', 0)"
        self.exec_sql('TESTDB', sql, {})

    def tearDown(self):
        sql = 'delete from share_uploadlinkshare'
        self.exec_sql('TESTDB', sql, {})
        sql = 'delete from share_fileshare'
        self.exec_sql('TESTDB', sql, {})
        sql = 'delete from tags_fileuuidmap'
        self.exec_sql('TESTDB', sql, {})

    def test_upload_link(self):
        changed_word = '/Picturesttt1'
        sql = "select count(*) from share_uploadlinkshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from share_uploadlinkshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_upload_link_with_chinese(self):
        changed_word = '/Picturestt测试'
        sql = "select count(*) from share_uploadlinkshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from share_uploadlinkshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_forlder_share_link_with_chinese(self):
        changed_word = '/Picturestt测试'
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_folder_share_link(self):
        changed_word = '/Picturesttt1'
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_file_share_link(self):
        changed_word = '/files/boy.jpg'
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/files/girl.jpg', changed_word, 1)
        sql = "select count(*) from share_fileshare where path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 1)

    def test_folder_uuidmap_link(self):
        changed_word = '/Picturesttt1'
        sql = "select count(*) from tags_fileuuidmap where parent_path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from tags_fileuuidmap where parent_path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_folder_uuidmap_link_with_chinese(self):
        changed_word = '/Picturesttt1编码'
        sql = "select count(*) from tags_fileuuidmap where parent_path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures', changed_word, 1)
        sql = "select count(*) from tags_fileuuidmap where parent_path like :path"
        param = {'path': changed_word + '%'}
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 3)

    def test_file_uuidmap_link(self):
        changed_word = '/Pictures/格莱美颁奖.jpg'
        d, fn = os.path.split(changed_word)
        sql = "select count(*) from tags_fileuuidmap where parent_path = :path"
        param = {'path': changed_word }
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 0)

        changer.update_db_records('c89409c5-b52c-4469-91ba-b222a5d3efff', '/Pictures/格莱美.jpg', changed_word, 0)
        sql = "select count(*) from tags_fileuuidmap where parent_path = :dir and filename = :filename"
        param = {'dir': d, 'filename': fn }
        res = self.query('TESTDB', sql, param, True)
        self.assertEqual(res[0], 1)
