import os
import logging

from seafevents.app.config import get_config

logger = logging.getLogger(__name__)


APP_NAME = 'semantic-search'

# sections
## indexManager worker count
INDEX_MANAGER_WORKERS = 2
INDEX_TASK_EXPIRE_TIME = 30 * 60

RETRIEVAL_NUM = 20

# embedding dimension
DIMENSION = 768

MODEL_VOCAB_PATH = ''
FILE_SENTENCE_LIMIT = 1000

THRESHOLD = 0.01

## seasearch
SEASEARCH_SERVER = 'http://127.0.0.1:4080'
SEASEARCH_TOKEN = ''
VECTOR_M = 256
SHARD_NUM = 1

## sea-embedding
SEA_EMBEDDING_SERVER = ''
SEA_EMBEDDING_KEY = ''


# repo file index support file types
SUPPORT_INDEX_FILE_TYPES = [
    '.sdoc',
    '.md',
    '.markdown',
    '.doc',
    '.docx',
    '.ppt',
    '.pptx',
    '.pdf',
]


CONF_DIR = '/opt/seafile/conf/'

try:
    import seahub.settings as seahub_settings
    SEA_EMBEDDING_SERVER = getattr(seahub_settings, 'SEA_EMBEDDING_SERVER', '')
    SEA_EMBEDDING_KEY = getattr(seahub_settings, 'SEA_EMBEDDING_KEY', '')
    SEASEARCH_SERVER = getattr(seahub_settings, 'SEASEARCH_SERVER', '')
    SEASEARCH_TOKEN = getattr(seahub_settings, 'SEASEARCH_TOKEN', '')
    MODEL_VOCAB_PATH = getattr(seahub_settings, 'MODEL_VOCAB_PATH', '')
    MODEL_CACHE_DIR = getattr(seahub_settings, 'MODEL_CACHE_DIR', '')
    INDEX_STORAGE_PATH = getattr(seahub_settings, 'INDEX_STORAGE_PATH', '')
except ImportError:
    logger.critical("Can not import seahub settings.")
    raise RuntimeError("Can not import seahub settings.")


try:

    if os.path.exists('/data/dev/seafevents/semantic_search/semantic_search_settings.py'):
        from seafevents.semantic_search.semantic_search_settings import *
except:
    pass
