# -*- coding: utf-8 -*-
import time
import sched
import logging
import datetime
from threading import Thread, Event

from sqlalchemy import delete

from seafevents.db import init_db_session_class
from seafevents.batch_delete_files_notice.models import DeletedFilesCount


logger = logging.getLogger(__name__)


__all__ = [
    'DeletedFilesCountCleaner',
]


class DeletedFilesCountCleaner(object):
    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)

    def start(self):
        DeletedFilesCountTask(self._db_session_class).start()


class DeletedFilesCountTask(Thread):
    def __init__(self, db_session_class):
        Thread.__init__(self)
        self._finished = Event()
        self._interval = 24*60*60
        self._db_session_class = db_session_class
        self._s = sched.scheduler(time.time, time.sleep)

    def clean(self, session):
        logger.info('Start clean delete_files_count')
        today = datetime.datetime.today()
        yesterday = (today - datetime.timedelta(days=1))
        session.execute(delete(DeletedFilesCount).where(DeletedFilesCount.deleted_time <= yesterday))
        session.commit()
        logger.info('Finished clean delete_files_count')

    def run(self):
        while not self._finished.is_set():
            self._finished.wait(self._interval)
            if not self._finished.is_set():
                session = self._db_session_class()
                try:
                    # run at 0 o'clock in every day
                    self._s.enter((24-time.localtime().tm_hour)*60*60, 0, self.clean, (session,))
                    self._s.run()
                except Exception as e:
                    logger.error(e)
                finally:
                    session.close()
