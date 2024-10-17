import json
import logging

from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import METADATA_TABLE, FACES_TABLE, query_metadata_rows, get_face_embeddings, face_compare

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

        metadata = self.metadata_server_api.get_metadata(repo_id)
        tables = metadata.get('tables', [])
        if not tables:
            return
        faces_table_id = [table['id'] for table in tables if table['name'] == FACES_TABLE.name][0]

        obj_id_to_rows = {}
        for item in query_result:
            obj_id = item[METADATA_TABLE.columns.obj_id.name]
            if obj_id not in obj_id_to_rows:
                obj_id_to_rows[obj_id] = []
            obj_id_to_rows[obj_id].append(item)

        obj_ids = list(obj_id_to_rows.keys())
        known_faces = []
        for obj_id in obj_ids:
            records = obj_id_to_rows.get(obj_id, [])
            known_faces = self.face_recognition(obj_id, records, repo_id, faces_table_id, known_faces)

    def face_recognition(self, obj_id, records, repo_id, faces_table_id, used_faces):
        embeddings = self.image_embedding_api.face_embeddings(repo_id, [obj_id]).get('data', [])
        if not embeddings:
            return used_faces
        embedding = embeddings[0]
        face_embeddings = embedding['embeddings']
        recognized_faces = []
        for face_embedding in face_embeddings:
            face = face_compare(face_embedding, used_faces, 1.24)
            if not face:
                row = {
                    FACES_TABLE.columns.vector.name: json.dumps(face_embedding),
                }
                result = self.metadata_server_api.insert_rows(repo_id, faces_table_id, [row])
                row_id = result.get('row_ids')[0]
                used_faces.append({
                    FACES_TABLE.columns.id.name: row_id,
                    FACES_TABLE.columns.vector.name: json.dumps(face_embedding),
                })
                row_id_map = {
                    row_id: [item.get(METADATA_TABLE.columns.id.name) for item in records]
                }
                self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, faces_table_id, row_id_map)
            else:
                recognized_faces.append(face)

        if recognized_faces:
            row_ids = [item[FACES_TABLE.columns.id.name] for item in recognized_faces]
            row_id_map = dict()
            for row in records:
                row_id_map[row[METADATA_TABLE.columns.id.name]] = row_ids
            self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, METADATA_TABLE.id, row_id_map)

        return used_faces
