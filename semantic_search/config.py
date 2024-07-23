import os
import logging

from seafevents.app.config import get_config
logger = logging.getLogger(__name__)

try:
    evtconf = os.environ['EVENTS_CONFIG_FILE']
    conf = get_config(evtconf)
    sem_conf = conf['SEMANTIC_SEARCH']

    MODEL_VOCAB_PATH = sem_conf['embedding_model_vocab_path']
    SUPPORT_INDEX_FILE_TYPES = sem_conf['suppport_index_file_types'].split(', ')

except Exception as e:
    logger.warning(e)
