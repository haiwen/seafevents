import os
import logging
from ConfigParser import ConfigParser
from datetime import timedelta
from datetime import datetime
from sqlalchemy.orm.scoping import scoped_session
from models import HistoryTotalStorageStat
from seafobj.objstore_factory import SeafObjStoreFactory

class HistoryTotalStorageCounter(object):
    def __init__(self, settings):
        self.settings = settings
        self.edb_session = scoped_session(settings.session_cls)
        settings.init_seafile_db()
        self.cursor = settings.seafile_cursor
        self.objstore_factory = SeafObjStoreFactory()

    def start_count(self):
        blocks_obj_store = self.objstore_factory.get_obj_store('blocks')

        try:
            self.cursor.execute('''SELECT repo_id FROM Repo
                                   WHERE repo_id NOT IN
                                   (SELECT repo_id FROM VirtualRepo)''')

            repo_ids = self.cursor.fetchall()
        except Exception as e:
            logging.warning('Failed to get repo_ids')

        for repo_id in repo_ids:
            block_obj_size = 0

            block_objs = blocks_obj_store.list_objs(repo_id)
            for block_obj in block_objs:
                block_obj_size += block_obj[2]

            dt = datetime.utcnow()
            _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
            timestamp = datetime.strptime(_timestamp,'%Y-%m-%d %H:%M:%S')

            try:
                q = self.edb_session.query(HistoryTotalStorageStat).filter(HistoryTotalStorageStat.repo_id==repo_id)
            except Exception as e:
                logging.warning('query error : %s.', e)

            try:
                r = q.first()
                if not r:
                    newrecord = HistoryTotalStorageStat(repo_id, timestamp, block_obj_size)
                    self.edb_session.add(newrecord)
                    self.edb_session.commit()
                elif r.timestamp != timestamp:
                    r.timestamp = timestamp
                    r.total_size = block_obj_size
                    self.edb_session.commit()

                self.edb_session.remove()
            except Exception as e:
                logging.warning('Failed to add record to HistoryTotalStorageStat: %s.', e)
