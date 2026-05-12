from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI


SUPPORTED_IMAGE_FORMATS = ('jpeg', 'jpg', 'heic', 'png', 'bmp', 'tif', 'tiff', 'jfif', 'jpe', 'ppm', 'heic')

def recognize_faces_by_obj_ids(repo_id, obj_ids):

    seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
    seafile_ai_api.recognize_faces(repo_id, obj_ids)
    return
    