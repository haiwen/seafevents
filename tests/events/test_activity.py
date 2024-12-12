# coding:utf8

import copy
import time
import pytest
import datetime

from sqlalchemy import select, delete

from seafevents.tests.utils import EventTest
from seafevents.events.db import save_user_activity, get_user_activities
from seafevents.events.models import Activity


@pytest.mark.usefixtures("test_db")
class AcvitityTest(EventTest):
    def setUp(self):
        session = self.get_session()
        session.execute(delete(Activity))
        session.commit()
        session.close()

        self.record = {
            'op_type': 'create',
            'obj_type': 'repo',
            'timestamp': datetime.datetime.utcnow(),
            'repo_id': 'fd62b808-63bf-4ab1-bdee-7fa4a94b85b5',
            'path': '/',
            'op_user': 'admin@admin.com',
            'related_users': ['test@test.com', 'admin@admin.com'],
            'org_id': None,
        }

    def tearDown(self):
        session = self.get_session()
        session.execute(delete(Activity))
        session.commit()
        session.close()

    def test_save_user_events(self):
        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        save_user_activity(session, self.record)
        session.close()

        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 1)
        session.close()

    def test_save_invalid_events(self):
        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        record = copy.copy(self.record)
        del(record['op_type'])
        try:
            save_user_activity(session, record)
        except KeyError:
            pass
        session.close()

        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 0)
        session.close()

    def test_save_extra_event(self):
        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        record = copy.copy(self.record)
        record['op_type'] = 'clear-up-trash'
        record['days'] = '0'
        save_user_activity(session, self.record)
        session.close()

        session = self.get_session()
        rows = session.scalars(select(Activity)).all()
        self.assertEqual(len(rows), 1)
        session.close()

    def test_get_user_activites(self):
        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 0, 2)
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        save_user_activity(session, self.record)
        session.close()

        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 0, 2)
        self.assertEqual(len(rows), 1)
        session.close()

    def test_get_user_activities_by_page(self):
        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 0, 2)
        self.assertEqual(len(rows), 0)
        session.close()

        session = self.get_session()
        save_user_activity(session, self.record)
        session.close()

        session = self.get_session()
        record = copy.copy(self.record)
        record['op_type'] = 'delete'
        time.sleep(1)
        record['timestamp'] = datetime.datetime.utcnow()
        save_user_activity(session, record)
        session.close()

        session = self.get_session()
        record = copy.copy(self.record)
        record['op_type'] = 'recover'
        time.sleep(1)
        record['timestamp'] = datetime.datetime.utcnow()
        save_user_activity(session, record)
        session.close()

        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 0, 3)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].op_type, 'recover')
        session.close()

        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 0, 2)
        self.assertEqual(len(rows), 2)
        session.close()

        session = self.get_session()
        rows = get_user_activities(session, 'admin@admin.com', 1, 1)
        self.assertEqual(len(rows), 1)
        session.close()
