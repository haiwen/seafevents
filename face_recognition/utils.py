import base64
import os
import json
import posixpath

from seaserv import seafile_api

from seafevents.repo_metadata.utils import query_metadata_rows, get_file_content
from seafevents.repo_metadata.constants import FACES_TABLE, METADATA_TABLE
from seafevents.face_recognition.constants import UNKNOWN_PEOPLE_NAME


VECTOR_DEFAULT_FLAG = '0'
FACE_EMBEDDING_DIM = 512
FACES_TMP_DIR = '/tmp'
FACES_SAVE_PATH = '_Internal/Faces'
EMBEDDING_UPDATE_LIMIT = 200
SUPPORTED_IMAGE_FORMATS = ('jpeg', 'jpg', 'heic', 'png', 'bmp', 'tif', 'tiff', 'jfif', 'jpe', 'ppm')


def feature_distance(feature1, feature2):
    import numpy as np
    diff = np.subtract(feature1, feature2)
    dist = np.sum(np.square(diff), 0)
    return dist


def b64encode_embeddings(embeddings):
    import numpy as np
    embedding_array = np.array(embeddings).astype(np.float32)
    encode = base64.b64encode(embedding_array.tobytes())
    return encode.decode('utf-8')


def b64decode_embeddings(encode):
    import numpy as np
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
    return nearest_cluster, min_distance


def get_faces_rows(repo_id, metadata_server_api):
    sql = f'SELECT * FROM `{FACES_TABLE.name}`'
    query_result = query_metadata_rows(repo_id, metadata_server_api, sql)
    clustered_rows = []
    unclustered_rows = []
    for row in query_result:
        if row.get(FACES_TABLE.columns.name.name) == UNKNOWN_PEOPLE_NAME:
            unclustered_rows.append(row)
        else:
            clustered_rows.append(row)
    return clustered_rows, unclustered_rows


def get_face_embeddings(repo_id, image_embedding_api, obj_ids):
    embeddings = []

    per_size = 50
    for i in range(0, len(obj_ids), per_size):
        query_results = image_embedding_api.face_embeddings(repo_id, obj_ids[i: i + per_size]).get('data', [])
        embeddings.append(query_results)

    return embeddings


def get_image_face(repo_id, obj_id, image_embedding_api, center=None):
    result = image_embedding_api.face_embeddings(repo_id, [obj_id], True).get('data', [])
    if not result:
        return None

    if len(result) == 1:
        return base64.b64decode(result[0]['faces'][0]['face'])

    faces = result[0]['faces']
    sim = [feature_distance(center, face['embedding']) for face in faces]
    return base64.b64decode(faces[sim.index(min(sim))]['face'])


def save_cluster_face(repo_id, related_row_ids, row_ids, id_to_record, cluster_center, face_row_id, image_embedding_api):
    face_image = None
    for row_id in related_row_ids:
        if row_ids.count(row_id) == 1:
            record = id_to_record[row_id]
            obj_id = record[METADATA_TABLE.columns.obj_id.name]
            face_image = get_image_face(repo_id, obj_id, image_embedding_api, cluster_center.tolist())
            break

    if not face_image:
        record = id_to_record[related_row_ids[0]]
        obj_id = record[METADATA_TABLE.columns.obj_id.name]
        face_image = get_image_face(repo_id, obj_id, image_embedding_api, cluster_center.tolist())

    if not face_image:
        return

    filename = f'{face_row_id}.jpg'
    save_face(repo_id, face_image, filename)


def get_min_cluster_size(faces_num):
    return max(faces_num // 100, 5)


def save_face(repo_id, image, filename, replace=False):
    tmp_content_path = posixpath.join(FACES_TMP_DIR, filename)
    with open(tmp_content_path, 'wb') as f:
        f.write(image)

    if replace:
        seafile_api.del_file(repo_id, FACES_SAVE_PATH, json.dumps([filename]), 'system')
    seafile_api.post_file(repo_id, tmp_content_path, FACES_SAVE_PATH, filename, 'system')
    os.remove(tmp_content_path)
