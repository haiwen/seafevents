import unittest

from sqlalchemy import text
from seafevents.tests.conftest import get_db_session

class EventTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def query(self, db, sql, param, oneres):
        session = None
        try:
            session = get_db_session(db)
            sqltext = text(sql)
            if oneres:
                res = session.execute(sqltext, param).fetchone()
            else:
                res = session.execute(sqltext, param).fetchall()
            return res
        finally:
            if session:
                session.close()

    def exec_sql(self, db, sql, param):
        session = None
        try:
            session = get_db_session(db)
            sqltext = text(sql)
            res = session.execute(sqltext, param)
            session.commit()
            return res
        finally:
            session.close()
