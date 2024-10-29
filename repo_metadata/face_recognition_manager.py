import json
import logging
from datetime import datetime

from sklearn.cluster import HDBSCAN
import numpy as np

from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import METADATA_TABLE, FACES_TABLE, query_metadata_rows, get_face_embeddings, get_faces_rows, get_cluster_by_center, update_face_cluster_time
from seafevents.repo_metadata.constants import METADATA_OP_LIMIT

logger = logging.getLogger(__name__)


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

    def init_face_recognition(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.file_type.name}` = "_picture"'

        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        obj_id_to_rows = {}
        for item in query_result:
            obj_id = item[METADATA_TABLE.columns.obj_id.name]
            if obj_id not in obj_id_to_rows:
                obj_id_to_rows[obj_id] = []
            obj_id_to_rows[obj_id].append(item)

        obj_ids = list(obj_id_to_rows.keys())
        updated_rows = []
        for i in range(0, len(obj_ids), 50):
            obj_ids_batch = obj_ids[i: i + 50]
            result = self.image_embedding_api.face_embeddings(repo_id, obj_ids_batch).get('data', [])
            if not result:
                continue

            for item in result:
                obj_id = item['obj_id']
                face_embeddings = item['embeddings']
                for row in obj_id_to_rows.get(obj_id, []):
                    row_id = row[METADATA_TABLE.columns.id.name]
                    updated_rows.append({
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.face_vectors.name: json.dumps(face_embeddings),
                    })
                    if len(updated_rows) >= METADATA_OP_LIMIT:
                        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                        updated_rows = []

        if updated_rows:
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

        self.face_cluster(repo_id)

    def face_cluster(self, repo_id):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_face_cluster_time(self._db_session_class, repo_id, current_time)

        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.face_vectors.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.face_vectors.name}` IS NOT NULL'
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
        for item in query_result:
            row_id = item[METADATA_TABLE.columns.id.name]
            face_vectors = json.loads(item[METADATA_TABLE.columns.face_vectors.name])
            for face_vector in face_vectors:
                vectors.append(face_vector)
                row_ids.append(row_id)

        old_cluster = get_faces_rows(repo_id, self.metadata_server_api)
        clt = HDBSCAN(min_cluster_size=5)
        clt.fit(vectors)

        label_ids = np.unique(clt.labels_)
        for label_id in label_ids:
            idxs = np.where(clt.labels_ == label_id)[0]
            related_row_ids = [row_ids[i] for i in idxs]
            if label_id != -1:
                cluster_center = np.mean([vectors[i] for i in idxs], axis=0)
                face_row = {
                    FACES_TABLE.columns.vector.name: json.dumps(cluster_center.tolist()),
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
            else:
                face_row = dict()

            result = self.metadata_server_api.insert_rows(repo_id, faces_table_id, [face_row])
            row_id = result.get('row_ids')[0]
            row_id_map = {
                row_id: related_row_ids
            }
            self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, faces_table_id, row_id_map)

        need_delete_row_ids = [item[FACES_TABLE.columns.id.name] for item in old_cluster]
        self.metadata_server_api.delete_rows(repo_id, faces_table_id, need_delete_row_ids)
