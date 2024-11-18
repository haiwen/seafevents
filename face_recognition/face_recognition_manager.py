import logging
import time
from datetime import datetime
import numpy as np

from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import METADATA_TABLE, FACES_TABLE, query_metadata_rows
from seafevents.repo_metadata.constants import METADATA_OP_LIMIT
from seafevents.face_recognition.db import update_face_cluster_time, update_face_cluster_time, get_repo_face_recognition_status
from seafevents.face_recognition.utils import get_faces_rows, get_cluster_by_center, b64encode_embeddings, \
    b64decode_embeddings, get_faces_rows, get_face_embeddings, get_image_face, save_face, VECTOR_DEFAULT_FLAG, \
    get_min_cluster_size

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
        face_recognition_status = get_repo_face_recognition_status(repo_id, self._db_session_class)
        return face_recognition_status

    def init_face_recognition(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.file_type.name}` = "_picture"'

        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        self.face_embeddings(repo_id, query_result)
        self.face_cluster(repo_id)

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
                    if len(updated_rows) >= METADATA_OP_LIMIT:
                        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                        updated_rows = []
                        logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
                        start_time = time.time()

        if updated_rows:
            logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def check_face_vectors(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.file_type.name}` = "_picture" AND `{METADATA_TABLE.columns.face_vectors.name}` IS NULL'

        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        self.face_embeddings(repo_id, query_result)
        logger.info('repo %s face vectors is completed', repo_id)

    def face_cluster(self, repo_id):
        try:
            from sklearn.cluster import HDBSCAN
        except ImportError:
            logger.warning('Package scikit-learn or opencv-python is not installed.')
            return

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_face_cluster_time(self._db_session_class, repo_id, current_time)

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
            id_to_record[item[METADATA_TABLE.columns.id.name]] = item
            row_id = item[METADATA_TABLE.columns.id.name]
            face_vectors = b64decode_embeddings(item[METADATA_TABLE.columns.face_vectors.name])
            for face_vector in face_vectors:
                vectors.append(face_vector)
                row_ids.append(row_id)

        old_cluster = get_faces_rows(repo_id, self.metadata_server_api)
        min_cluster_size = get_min_cluster_size(len(vectors))
        clt = HDBSCAN(min_cluster_size=min_cluster_size)
        clt.fit(vectors)

        label_ids = np.unique(clt.labels_)
        for label_id in label_ids:
            if label_id == -1:
                continue

            idxs = np.where(clt.labels_ == label_id)[0]
            related_row_ids = [row_ids[i] for i in idxs]

            cluster_center = np.mean([vectors[i] for i in idxs], axis=0)
            face_row = {
                FACES_TABLE.columns.vector.name: b64encode_embeddings(cluster_center.tolist()),
            }
            cluster = get_cluster_by_center(cluster_center, old_cluster)
            if cluster:
                cluster_id = cluster[FACES_TABLE.columns.id.name]
                old_cluster = [item for item in old_cluster if item[FACES_TABLE.columns.id.name] != cluster_id]
                face_row[FACES_TABLE.columns.id.name] = cluster_id
                self.metadata_server_api.update_rows(repo_id, faces_table_id, [face_row])
                row_id_map = {
                    cluster_id: related_row_ids
                }
                self.metadata_server_api.update_link(repo_id, FACES_TABLE.link_id, faces_table_id, row_id_map)
                continue

            result = self.metadata_server_api.insert_rows(repo_id, faces_table_id, [face_row])
            face_row_id = result.get('row_ids')[0]
            row_id_map = {
                face_row_id: related_row_ids
            }
            self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, faces_table_id, row_id_map)

            face_image = None
            for row_id in related_row_ids:
                if row_ids.count(row_id) == 1:
                    record = id_to_record[row_id]
                    obj_id = record[METADATA_TABLE.columns.obj_id.name]
                    face_image = get_image_face(repo_id, obj_id, self.image_embedding_api, cluster_center.tolist())
                    break

            if face_image is None:
                record = id_to_record[related_row_ids[0]]
                obj_id = record[METADATA_TABLE.columns.obj_id.name]
                face_image = get_image_face(repo_id, obj_id, self.image_embedding_api, cluster_center.tolist())

            if face_image is None:
                continue

            filename = f'{face_row_id}.jpg'
            save_face(repo_id, face_image, filename)
