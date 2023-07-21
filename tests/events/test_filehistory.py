# coding:utf8

import copy
import time
import pytest
import datetime

from sqlalchemy import delete

from seafevents.tests.utils import EventTest, save_file_history
from seafevents.events.db import get_file_history
from seafevents.events.models import FileHistory


@pytest.mark.usefixtures("test_db")
class AcvitityTest(EventTest):
    def setUp(self):
        self.remove_data()
        self.repo_id = 'fd62b808-63bf-4ab1-bdee-7fa4a94b85b5'
        self.path = '/test.txt'

        self.record = {
            'op_type': 'create',
            'obj_type': 'file',
            'timestamp': datetime.datetime.utcnow(),
            'repo_id': self.repo_id,
            'path': self.path,
            'op_user': 'admin@admin.com',
            'related_users': ['test@test.com', 'admin@admin.com'],
            'org_id': None,
            'obj_id': '0000',
            'size': 0
        }

    def tearDown(self):
        self.remove_data()

    def remove_data(self):
        session = self.get_session()
        session.execute(delete(FileHistory))
        session.commit()
        session.close()

    def test_paging(self):
        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, self.path, 0, 10)
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        record = copy.deepcopy(self.record)
        save_file_history(session, record)
        session.close()

        time.sleep(2)
        session = self.get_session()
        record = copy.deepcopy(self.record)
        record['op_type'] = 'edit'
        record['timestamp'] = datetime.datetime.utcnow()
        save_file_history(session, record)
        session.close()

        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, self.path, 0, 10)
        self.assertEqual(len(rows), 2)
        self.assertEqual('edit', rows[0].op_type)
        self.assertEqual(rows[0].file_uuid, rows[1].file_uuid)
        session.close()

        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, self.path, 1, 10)
        self.assertEqual(len(rows), 1)
        session.close()

    def test_file_uuid(self):
        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, self.path, 0, 10)
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        record = copy.deepcopy(self.record)
        save_file_history(session, record)
        session.close()

        time.sleep(1)
        session = self.get_session()
        record = copy.deepcopy(self.record)
        renamed_path = '/renamed.doc'
        record['op_type'] = 'rename'
        record['path'] = renamed_path
        record['old_path'] = self.path
        record['timestamp'] = datetime.datetime.utcnow()
        save_file_history(session, record)
        session.close()
        time.sleep(1)

        session = self.get_session()
        record = copy.deepcopy(self.record)
        new_file_path = '/test.md'
        record['path'] = new_file_path
        record['timestamp'] = datetime.datetime.utcnow()
        save_file_history(session, record)
        session.close()

        session = self.get_session()
        record = copy.deepcopy(self.record)
        record['op_type'] = 'recover'
        record['path'] = self.path
        record['timestamp'] = datetime.datetime.utcnow()
        save_file_history(session, record)
        session.close()

        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, renamed_path, 0, 10)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].file_uuid, rows[1].file_uuid)
        session.close()

        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, new_file_path, 0, 10)
        self.assertEqual(len(rows), 1)
        session.close()

        session = self.get_session()
        rows, total_count = get_file_history(session, self.repo_id, self.path, 0, 10)
        self.assertEqual(len(rows), 1)
        session.close()
