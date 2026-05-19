import logging
import os
import time
from datetime import datetime, timedelta
import json

from sqlalchemy.sql import text

from seafevents.db import init_db_session_class
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.face_recognition.utils import SUPPORTED_IMAGE_FORMATS
from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL



from seaserv import seafile_api

logger = logging.getLogger('face_recognition')


class FaceRecognitionManager(object):

    def __init__(self):
        self._db_session_class = init_db_session_class()
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)

    def check_face_recognition_status(self, repo_id):
        if not self.seafile_ai_api:
            return None
        with self._db_session_class() as session:
            sql = "SELECT face_recognition_enabled FROM repo_metadata WHERE repo_id='%s'" % repo_id
            record = session.execute(text(sql)).fetchone()

        return record[0] if record else None

    def is_support_format(self, suffix):
        if suffix.lower() in SUPPORTED_IMAGE_FORMATS:
            return True

        return False
    def face_embeddings_by_obj_ids(self, repo_id, obj_ids, need_classify=False):
        self.seafile_ai_api.face_batch_embeddings(repo_id, obj_ids, need_classify)
    
    def face_cluster(self, repo_id):
        self.seafile_ai_api.face_cluster(repo_id)
        return
        
    def update_face_cluster(self, repo_id, username=None):
        logger.info('Updating face cluster repo %s' % repo_id)
        start_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.face_cluster(repo_id)
        self.finish_face_cluster(repo_id, start_update_time)

        if username:
            self.save_face_cluster_message_to_user_notification(repo_id, username)
        logger.info('Finish face cluster repo %s' % repo_id)

    def get_pending_face_cluster_repo_list(self, start, count):
        per_day_check_time = datetime.now() - timedelta(hours=23)
        with self._db_session_class() as session:
            cmd = """SELECT repo_id, last_face_cluster_time FROM repo_metadata WHERE face_recognition_enabled = True
            AND (last_face_cluster_time < :per_day_check_time OR last_face_cluster_time IS NULL) limit :start, :count"""
            res = session.execute(text(cmd),
                                  {'start': start, 'count': count, 'per_day_check_time': per_day_check_time}).fetchall()

        return res

    def finish_face_cluster(self, repo_id, start_update_time):
        with self._db_session_class() as session:
            cmd = """UPDATE repo_metadata SET last_face_cluster_time = :update_time WHERE repo_id = :repo_id"""
            session.execute(text(cmd), {'update_time': start_update_time, 'repo_id': repo_id})
            session.commit()

    def save_face_cluster_message_to_user_notification(self, repo_id, username):
        values = []
        repo = seafile_api.get_repo(repo_id)
        repo_name = repo.repo_name
        detail = {
            'repo_id': repo_id,
            'repo_name': repo_name,
            'op_user': username,
            'op_type': 'init_face_cluster',
        }
        msg_type = 'face_cluster'
        local_datetime_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        detail = json.dumps(detail).replace('\\', '\\\\')

        values.append((username, msg_type, detail, local_datetime_str, 0))
        with self._db_session_class() as session:
            sql = """INSERT INTO notifications_usernotification (to_user, msg_type, detail, timestamp, seen)
                                             VALUES %s""" % ', '.join(
                ["('%s', '%s', '%s', '%s', %s)" % value for value in values])
            session.execute(text(sql))
            session.commit()

    def update_people_cover_photo(self, repo_id, people_id, path, download_token):
        self.seafile_ai_api.update_people_cover_photo(repo_id, people_id, path, download_token)
        return 
