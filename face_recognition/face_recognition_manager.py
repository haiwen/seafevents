import logging
import time
from datetime import datetime, timedelta
import json

from sqlalchemy.sql import text

from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import query_metadata_rows, get_metadata_by_row_ids
from seafevents.repo_metadata.constants import METADATA_TABLE, FACES_TABLE
from seafevents.face_recognition.constants import UNKNOWN_PEOPLE_NAME
from seafevents.face_recognition.utils import get_faces_rows, get_cluster_by_center, b64encode_embeddings, \
    b64decode_embeddings, VECTOR_DEFAULT_FLAG, get_min_cluster_size, SUPPORTED_IMAGE_FORMATS, EMBEDDING_UPDATE_LIMIT, \
    save_cluster_face, get_image_face, save_face

from seaserv import seafile_api

logger = logging.getLogger('face_recognition')


class FaceRecognitionManager(object):

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.image_embedding_api = None

        self._parse_config(config)

    def _parse_config(self, config):
        ai_section_name = 'AI'
        if config.has_section(ai_section_name):
            image_embedding_service_url = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_service_url')
            image_embedding_secret_key = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_secret_key')
            self.image_embedding_api = ImageEmbeddingAPI(image_embedding_service_url, image_embedding_secret_key)

    def check_face_recognition_status(self, repo_id):
        if not self.image_embedding_api:
            return None
        with self._db_session_class() as session:
            sql = "SELECT face_recognition_enabled FROM repo_metadata WHERE repo_id='%s'" % repo_id
            record = session.execute(text(sql)).fetchone()

        return record[0] if record else None

    def is_support_format(self, suffix):
        if suffix in SUPPORTED_IMAGE_FORMATS:
            return True

        return False

    def face_embeddings(self, repo_id, rows):
        logger.info('repo %s need update face_vectors rows count: %d', repo_id, len(rows))
        obj_id_to_rows = {}
        for item in rows:
            obj_id = item[METADATA_TABLE.columns.obj_id.name]
            if obj_id not in obj_id_to_rows:
                obj_id_to_rows[obj_id] = []
            obj_id_to_rows[obj_id].append(item)

        obj_ids = list(obj_id_to_rows.keys())
        updated_rows = []
        start_time = time.time()
        for i in range(0, len(obj_ids), 50):
            obj_ids_batch = obj_ids[i: i + 50]
            result = self.image_embedding_api.face_embeddings(repo_id, obj_ids_batch).get('data', [])
            if not result:
                continue

            for item in result:
                obj_id = item['obj_id']
                face_embeddings = [face['embedding'] for face in item['faces']]
                vector = b64encode_embeddings(face_embeddings) if face_embeddings else VECTOR_DEFAULT_FLAG
                for row in obj_id_to_rows.get(obj_id, []):
                    row_id = row[METADATA_TABLE.columns.id.name]
                    updated_rows.append({
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.face_vectors.name: vector,
                    })
                    if len(updated_rows) >= EMBEDDING_UPDATE_LIMIT:
                        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                        logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
                        start_time = time.time()
                        updated_rows = []

        if updated_rows:
            logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def ensure_face_vectors(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.suffix.name}` in {SUPPORTED_IMAGE_FORMATS} AND `{METADATA_TABLE.columns.face_vectors.name}` IS NULL'

        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        self.face_embeddings(repo_id, query_result)
        logger.info('repo %s face vectors is completed', repo_id)

    def face_cluster(self, repo_id):
        try:
            from sklearn.cluster import HDBSCAN
            import numpy as np
        except ImportError:
            logger.warning('Package scikit-learn is not installed. ')
            return
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.face_vectors.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.face_vectors.name}` IS NOT NULL AND `{METADATA_TABLE.columns.face_vectors.name}` <> "{VECTOR_DEFAULT_FLAG}"'
        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        metadata = self.metadata_server_api.get_metadata(repo_id)
        tables = metadata.get('tables', [])
        if not tables:
            return
        faces_table_id = [table['id'] for table in tables if table['name'] == FACES_TABLE.name][0]

        vectors = []
        row_ids = []
        id_to_record = dict()
        for item in query_result:
            row_id = item[METADATA_TABLE.columns.id.name]
            id_to_record[row_id] = item
            face_vectors = b64decode_embeddings(item[METADATA_TABLE.columns.face_vectors.name])
            for face_vector in face_vectors:
                vectors.append(face_vector)
                row_ids.append(row_id)

        culstered_rows, unclustered_rows = get_faces_rows(repo_id, self.metadata_server_api)
        min_cluster_size = get_min_cluster_size(len(vectors))
        if len(vectors) < min_cluster_size:
            clt_labels = [-1] * len(vectors)
        else:
            clt = HDBSCAN(min_cluster_size=min_cluster_size)
            clt.fit(vectors)
            clt_labels = clt.labels_

        cluster_id_to_min_distance = {}
        label_id_to_added_cluster = {}
        label_id_to_updated_cluster = {}
        cluster_id_to_label = {}
        label_ids = np.unique(clt_labels)
        for label_id in label_ids:
            idxs = np.where(clt_labels == label_id)[0]
            related_row_ids = [row_ids[i] for i in idxs]

            if label_id == -1:
                if not unclustered_rows:
                    label_id_to_added_cluster[label_id] = ({
                        FACES_TABLE.columns.name.name: UNKNOWN_PEOPLE_NAME,
                    }, related_row_ids, None)
                else:
                    cluster_id = unclustered_rows[0][FACES_TABLE.columns.id.name]
                    label_id_to_updated_cluster[label_id] = ({}, related_row_ids, cluster_id, None)

                continue

            cluster_center = np.mean([vectors[i] for i in idxs], axis=0)
            face_row = {
                FACES_TABLE.columns.vector.name: b64encode_embeddings(cluster_center.tolist()),
            }
            old_cluster, distance = get_cluster_by_center(cluster_center, culstered_rows)
            if old_cluster:
                cluster_id = old_cluster[FACES_TABLE.columns.id.name]
                old_distance = cluster_id_to_min_distance.get(cluster_id)
                if old_distance:
                    if old_distance > distance:
                        label_id_to_updated_cluster[label_id] = (face_row, related_row_ids, cluster_id, cluster_center)
                        old_label_id = cluster_id_to_label.get(cluster_id)
                        old_cluster_info = label_id_to_updated_cluster.pop(old_label_id)
                        cluster_id_to_min_distance[cluster_id] = distance
                        label_id_to_added_cluster[old_label_id] = (old_cluster_info[0], old_cluster_info[1], old_cluster_info[3])
                    else:
                        label_id_to_added_cluster[label_id] = (face_row, related_row_ids, cluster_center)
                else:
                    label_id_to_updated_cluster[label_id] = (face_row, related_row_ids, cluster_id, cluster_center)
                    cluster_id_to_label[cluster_id] = label_id
                    cluster_id_to_min_distance[cluster_id] = distance
                continue
            label_id_to_added_cluster[label_id] = (face_row, related_row_ids, cluster_center)

        for value in label_id_to_updated_cluster.values():
            face_row, related_row_ids, cluster_id, _ = value
            if face_row:
                face_row[FACES_TABLE.columns.id.name] = cluster_id
                self.metadata_server_api.update_rows(repo_id, faces_table_id, [face_row])
            exist_rows = get_metadata_by_row_ids(repo_id, related_row_ids, self.metadata_server_api)
            row_id_map = {
                cluster_id: [item[METADATA_TABLE.columns.id.name] for item in exist_rows]
            }
            self.metadata_server_api.update_link(repo_id, FACES_TABLE.face_link_id, faces_table_id, row_id_map)

        for value in label_id_to_added_cluster.values():
            face_row, related_row_ids, cluster_center = value
            result = self.metadata_server_api.insert_rows(repo_id, faces_table_id, [face_row])
            face_row_id = result.get('row_ids')[0]
            exist_rows = get_metadata_by_row_ids(repo_id, related_row_ids, self.metadata_server_api)
            row_id_map = {
                face_row_id: [item[METADATA_TABLE.columns.id.name] for item in exist_rows]
            }
            self.metadata_server_api.insert_link(repo_id, FACES_TABLE.face_link_id, faces_table_id, row_id_map)

            if cluster_center is None:
                continue

            # save a cover for new face cluster.
            save_cluster_face(repo_id, related_row_ids, row_ids, id_to_record, cluster_center, face_row_id, self.image_embedding_api)

    def update_face_cluster(self, repo_id, username=None):
        logger.info('Updating face cluster repo %s' % repo_id)
        start_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.ensure_face_vectors(repo_id)
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

    def update_people_cover_photo(self, repo_id, people_id, obj_id):
        face_image = get_image_face(repo_id, obj_id, self.image_embedding_api, center=None)
        filename = f'{people_id}.jpg'
        save_face(repo_id, face_image, filename, replace=True)
