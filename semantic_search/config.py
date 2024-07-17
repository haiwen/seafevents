import os
import logging

from seafevents.app.config import get_config
logger = logging.getLogger(__name__)

try:
    evtconf = os.environ['EVENTS_CONFIG_FILE']
    conf = get_config(evtconf)
    sem_conf = conf['SEMANTIC_SEARCH']

    INDEX_MANAGER_WORKERS = int(sem_conf['index_manager_workers'])
    INDEX_TASK_EXPIRE_TIME = int(sem_conf['index_task_expire_time'])
    RETRIEVAL_NUM = int(sem_conf['retrieval_num'])
    DIMENSION = int(sem_conf['embedding_dimension'])
    MODEL_VOCAB_PATH = sem_conf['embedding_model_vocab_path']
    FILE_SENTENCE_LIMIT = int(sem_conf['embedding_file_sentence_limit'])
    THRESHOLD = float(sem_conf['threshold'])
    SEASEARCH_SERVER = sem_conf['seasearch_server']
    SEASEARCH_TOKEN = sem_conf['seasearch_token']
    VECTOR_M = int(sem_conf['seasearch_vector_m'])
    SHARD_NUM = int(sem_conf['seasearch_shard_num'])
    SUPPORT_INDEX_FILE_TYPES = sem_conf['suppport_index_file_types'].split(', ')
    SEA_EMBEDDING_SERVER = sem_conf['sea_embedding_server']
    SEA_EMBEDDING_KEY = sem_conf['sea_embedding_key']
except Exception as e:
    logger.warning(e)
