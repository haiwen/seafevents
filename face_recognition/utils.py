import base64
import os
import posixpath

import numpy as np
from seaserv import seafile_api

from seafevents.repo_metadata.utils import FACES_TABLE, query_metadata_rows, get_file_content


VECTOR_DEFAULT_FLAG = '0'
FACE_EMBEDDING_DIM = 512
FACES_TMP_DIR = '/tmp'
FACES_SAVE_PATH = '_Internal/Faces'


def feature_distance(feature1, feature2):
    diff = np.subtract(feature1, feature2)
    dist = np.sum(np.square(diff), 0)
    return dist


def b64encode_embeddings(embeddings):
    embedding_array = np.array(embeddings).astype(np.float32)
    encode = base64.b64encode(embedding_array.tobytes())
    return encode.decode('utf-8')


def b64decode_embeddings(encode):
    decode = base64.b64decode(encode)
    embedding_array = np.frombuffer(decode, dtype=np.float32)
    face_num = len(embedding_array) // FACE_EMBEDDING_DIM
    embedding = embedding_array.reshape((face_num, FACE_EMBEDDING_DIM)).tolist()
    return embedding


def get_cluster_by_center(center, clusters):
    min_distance = float('inf')
    nearest_cluster = None
    for cluster in clusters:
        vector = cluster.get(FACES_TABLE.columns.vector.name)
        if not vector:
            continue

        vector = b64decode_embeddings(vector)[0]
        distance = feature_distance(center, vector)
        if distance < 1 and distance < min_distance:
            min_distance = distance
            nearest_cluster = cluster
    return nearest_cluster


def get_faces_rows(repo_id, metadata_server_api):
    sql = f'SELECT * FROM `{FACES_TABLE.name}`'
    query_result = query_metadata_rows(repo_id, metadata_server_api, sql)
    return query_result if query_result else []


def get_face_embeddings(repo_id, image_embedding_api, obj_ids):
    embeddings = []

    per_size = 50
    for i in range(0, len(obj_ids), per_size):
        query_results = image_embedding_api.face_embeddings(repo_id, obj_ids[i: i + per_size]).get('data', [])
        embeddings.append(query_results)

    return embeddings


def get_image_face(repo_id, obj_id, image_embedding_api, center):
    result = image_embedding_api.face_embeddings(repo_id, [obj_id], True).get('data', [])
    if not result:
        return None

    if len(result) == 1:
        return base64.b64decode(result[0]['faces'][0]['face'])

    faces = result[0]['faces']
    sim = [feature_distance(center, face['embedding']) for face in faces]
    return base64.b64decode(faces[min(sim)]['face'])


def get_min_cluster_size(faces_num):
    return max(faces_num // 100, 5)


def save_face(repo_id, image, filename):
    tmp_content_path = posixpath.join(FACES_TMP_DIR, filename)
    with open(tmp_content_path, 'wb') as f:
        f.write(image)

    seafile_api.post_file(repo_id, tmp_content_path, FACES_SAVE_PATH, filename, 'system')
    os.remove(tmp_content_path)
