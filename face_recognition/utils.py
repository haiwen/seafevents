import base64
import io
import json
import numpy as np
import requests
from PIL import Image

from seaserv import seafile_api

from seafevents.repo_metadata.utils import FACES_TABLE, query_metadata_rows, get_file_content
from seafevents.repo_metadata.constants import FACE_EMBEDDING_DIM

from seafevents.app.config import FILE_SERVER_ROOT


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
    result = image_embedding_api.face_embeddings(repo_id, [obj_id]).get('data', [])
    if not result:
        return None

    if len(result) == 1:
        return get_face_by_box(repo_id, obj_id, result[0]['faces'][0]['box'])

    faces = result[0]['faces']
    sim = [feature_distance(center, face['embedding']) for face in faces]
    return get_face_by_box(repo_id, obj_id, faces[min(sim)]['box'])


def get_face_by_box(repo_id, obj_id, box):
    content = get_file_content(repo_id, obj_id)
    if not content:
        return None

    image = Image.open(io.BytesIO(content))
    cropped_image = image.crop((box[0], box[1], box[2], box[3]))
    output_buffer = io.BytesIO()
    cropped_image.save(output_buffer, format='jpeg')
    output_buffer.seek(0)

    return output_buffer.getvalue()


def save_face(repo_id, parent_dir, image, filename):
    obj_id = json.dumps({'parent_dir': parent_dir})
    token = seafile_api.get_fileserver_access_token(repo_id, obj_id, 'upload', 'system', use_onetime=False)
    upload_link = gen_file_upload_url(token, 'upload-aj')

    response = requests.post(upload_link, files={'file': (filename, image)}, data={
        'parent_dir': parent_dir,
    }, timeout=30)
    if response.status_code != 200:
        raise ConnectionError(response.status_code, response.text)


def gen_file_upload_url(token, op, replace=False):
    url = '%s/%s/%s' % (FILE_SERVER_ROOT, op, token)
    if replace is True:
        url += '?replace=1'
    return url
