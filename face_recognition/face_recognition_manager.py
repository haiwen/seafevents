import logging
import time
from datetime import datetime
import numpy as np

from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import query_metadata_rows
from seafevents.repo_metadata.constants import METADATA_TABLE, FACES_TABLE
from seafevents.face_recognition.db import update_face_cluster_time, update_face_cluster_time, get_repo_face_recognition_status
from seafevents.face_recognition.utils import get_faces_rows, get_cluster_by_center, b64encode_embeddings, \
    b64decode_embeddings, get_faces_rows, get_face_embeddings, get_image_face, save_face, VECTOR_DEFAULT_FLAG, \
    get_min_cluster_size, SUPPORTED_IMAGE_FORMATS, EMBEDDING_UPDATE_LIMIT

logger = logging.getLogger('face_recognition')

from seafevents.repo_metadata.utils import METADATA_TABLE, get_file_type_ext_by_name
import os
import json
from sqlalchemy.sql import text
from seafevents.repo_metadata.metadata_manager import get_diff_files
import pymysql
from seaserv import seafile_api
from datetime import timedelta

EXCLUDED_PATHS = ['/_Internal', '/images']


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

    def is_support_format(self, suffix):
        if suffix in SUPPORTED_IMAGE_FORMATS:
            return True

        return False

    def init_face_recognition(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.suffix.name}` in {SUPPORTED_IMAGE_FORMATS}'

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
                    if len(updated_rows) >= EMBEDDING_UPDATE_LIMIT:
                        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                        logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
                        start_time = time.time()
                        updated_rows = []

        if updated_rows:
            logger.info('repo %s updated face_vectors rows count: %d, cost time: %.2f', repo_id, len(updated_rows), time.time() - start_time)
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def check_face_vectors(self, repo_id):
        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.suffix.name}` in {SUPPORTED_IMAGE_FORMATS} AND `{METADATA_TABLE.columns.face_vectors.name}` IS NULL'

        query_result = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        if not query_result:
            return

        self.face_embeddings(repo_id, query_result)
        logger.info('repo %s face vectors is completed', repo_id)

    def face_cluster(self, repo_id):
        try:
            from sklearn.cluster import HDBSCAN
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
            # b64decode_embeddings 这步需不需放到循环外面执行？get_cluster_by_center 中的
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

            if not face_image:
                record = id_to_record[related_row_ids[0]]
                obj_id = record[METADATA_TABLE.columns.obj_id.name]
                face_image = get_image_face(repo_id, obj_id, self.image_embedding_api, cluster_center.tolist())

            if not face_image:
                continue

            filename = f'{face_row_id}.jpg'
            save_face(repo_id, face_image, filename)

    def update_face_cluster(self, repo_id, face_commit, metadata_from_commit, face_creator):
        # 更新到某一个时，可能这个资料库已经关闭的face_cluster
        need_cluster = self.check_need_face_cluster(repo_id, face_commit, metadata_from_commit)
        print('need_cluster')
        print(need_cluster)
        if not need_cluster:
            # 如果更新的话，那么新增图片后只能等23小时后才能再更新，不更新的话，查询时又会每次都检查这个资料库
            # self._face_recognition_manager.finish_face_cluster(repo_id, latest_commit_id)
            return
        # self._face_recognition_manager.check_face_vectors(repo_id)
        latest_commit_id = self.get_latest_metadata_commit_id(repo_id)
        self.check_face_vectors(repo_id)  # 这步比较耗时，获取latest_commit_id的应该放到上面？
        # 在这步进行的过程中有可能slow_task 中也添加了一些face_vector吧？
        # latest_commit_id = self.get_latest_commit_id(repo_id)
        # 为后面的更新使用，放在这里是因为挨着查询，下面的运算可能比较耗时，时间太长就不准了，也不适合放到 check_need_face_cluster 中，
        # 因为后面还有ensure_face_vectors，这步会调用face_embedding耗时会较长，当然这步也是在 latest_commit_id 里的，
        # 所以可以放 check_need_face_cluster 中
        self.face_cluster(repo_id)
        # logger.warning('Package scikit-learn is not installed. ')
        # face_cluster 中的这步判断是不是应该放到整个项目启动定时任务中判断呢？判断是否enable中？ 这样更合理吧，否则没配的话，每次都要空跑这个程序
        self.finish_face_cluster(repo_id, latest_commit_id)

        if not face_commit:
            # 第一次开启时发送一个通知，关闭face时，需要删除face_commit
            self.save_face_cluster_message_to_user_notification(repo_id, face_creator)

    def get_pending_face_cluster_repo_list(self, start, count):
        # 需不需要加order_by? order_by 可以根据repo_id
        per_day_check_time = datetime.now() - timedelta(hours=23)
        with self._db_session_class() as session:
            cmd = """SELECT repo_id, face_creator, face_commit, from_commit FROM repo_metadata WHERE face_recognition_enabled = True 
            AND (last_face_cluster_time < :per_day_check_time OR last_face_cluster_time IS NULL) limit :start, :count"""
            res = session.execute(text(cmd),
                                  {'start': start, 'count': count, 'per_day_check_time': per_day_check_time}).fetchall()

        return res

    def get_latest_metadata_commit_id(self, repo_id):
        with self._db_session_class() as session:
            cmd = """SELECT from_commit FROM repo_metadata WHERE face_recognition_enabled = True AND repo_id = :repo_id"""
            res = session.execute(text(cmd), {'repo_id': repo_id}).fetchone()

        return res[0] if res else None

    def finish_face_cluster(self, repo_id, new_face_commit):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with self._db_session_class() as session:
            cmd = """UPDATE repo_metadata SET last_face_cluster_time = :update_time, face_commit = :face_commit WHERE repo_id = :repo_id"""
            session.execute(text(cmd), {'update_time': current_time, 'repo_id': repo_id, 'face_commit': new_face_commit})
            session.commit()

    def is_excluded_path(self, path):
        if not path or path == '/':
            return True
        for ex_path in EXCLUDED_PATHS:
            if path.startswith(ex_path):
                return True

    def check_need_face_cluster(self, repo_id, face_commit, metadata_from_commit):
        # 这里可以改成和最新from_commit 进行diff, （每次1000个到990个的时候 commit已经落后很多了），当然不用最新的也可以，只要更新用的是最新的就可以了
        if face_commit == metadata_from_commit:
            return False
        files = get_diff_files(repo_id, face_commit, metadata_from_commit)
        if not files:
            return False
        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, _, _, \
        _, _ = files

        if not added_files and not deleted_files:
            return False

        for file in (added_files + deleted_files):
            path = file.path.rstrip('/')
            file_name = os.path.basename(path)
            file_type, file_ext = get_file_type_ext_by_name(file_name)
            #
            if self.is_excluded_path(path):
                continue
            if file_type == '_picture':
                return True
        return False

    def save_face_cluster_message_to_user_notification(self, repo_id, op_user):
        values = []
        repo = seafile_api.get_repo(repo_id)
        repo_name = repo.repo_name
        detail = {
            'repo_id': repo_id,
            'repo_name': repo_name,
            'op_user': op_user,
            'op_type': 'init_face_cluster',
        }
        msg_type = 'face_cluster'
        local_datetime_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        detail = json.dumps(detail)

        values.append((op_user, msg_type, detail, local_datetime_str, 0))
        with self._db_session_class() as session:
            sql = """INSERT INTO notifications_usernotification (to_user, msg_type, detail, timestamp, seen)
                                             VALUES %s""" % ', '.join(
                ["('%s', '%s', '%s', '%s', %s)" % value for value in values])
            session.execute(text(sql))
            session.commit()
