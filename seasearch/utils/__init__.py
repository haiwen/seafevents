import logging
import hashlib
import json
import sys
import os
from sqlalchemy import text

from seafevents.seasearch.utils.commit_differ import CommitDiffer

from seafobj import fs_mgr, commit_mgr
from seafobj.exceptions import GetObjectError

logger = logging.getLogger(__name__)

SYS_DIRS = ['images', '_Internal']
WIKI_DIRS = ['wiki-pages']

def get_library_diff_files(repo_id, old_commit_id, new_commit_id):
    if old_commit_id == new_commit_id:
        return [], [], [], [], []

    old_root = None
    if old_commit_id:
        try:
            old_commit = commit_mgr.load_commit(repo_id, 0, old_commit_id)
            old_root = old_commit.root_id
        except GetObjectError as e:
            logger.debug(e)
            old_root = None

    try:
        new_commit = commit_mgr.load_commit(repo_id, 0, new_commit_id)
    except GetObjectError as e:
        # new commit should exists in the obj store
        logger.warning(e)
        return [], [], [], [], []

    new_root = new_commit.root_id
    version = new_commit.get_version()

    try:
        differ = CommitDiffer(repo_id, version, old_root, new_root)
        added_files, deleted_files, added_dirs, deleted_dirs, modified_files = differ.diff(new_commit.ctime)
    except Exception as e:
        logger.warning('repo: %s, version: %s, old_commit_id:%s, nea_commit_id: %s, old_root:%s, new_root: %s, differ error: %s',
                        repo_id, version, old_commit_id, new_commit_id, old_root, new_root, e)
        return [], [], [], [], []

    return added_files, deleted_files, modified_files, added_dirs, deleted_dirs


def init_logging(args):
    level = args.loglevel

    if level == 'debug':
        level = logging.DEBUG
    elif level == 'info':
        level = logging.INFO
    elif level == 'warning':
        level = logging.WARNING
    else:
        level = logging.INFO

    try:
        # set boto3 log level
        import boto3
        boto3.set_stream_logger(level=logging.WARNING)
    except:
        pass

    seafile_log_to_stdout = os.getenv('SEAFILE_LOG_TO_STDOUT', 'false') == 'true'
    stream = args.logfile
    format = '[%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(funcName)s %(message)s'
    if seafile_log_to_stdout:
        stream = sys.stdout
        format = '[seafevents] [%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(funcName)s %(message)s'
    kw = {
        # 'format': '[seafevents] [%(asctime)s] [%(levelname)s] %(message)s',
        'format': format,
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': level,
        'stream': stream
    }

    logging.basicConfig(**kw)
    logging.getLogger('oss_util').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def md5(text):
    return hashlib.md5(text.encode()).hexdigest()


def is_sys_dir_or_file(path):
    if path.split('/')[1] in SYS_DIRS:
        return True
    return False


def need_index_metadata_info(repo_id, session):
    with session() as session:
        sql = "SELECT enabled FROM repo_metadata WHERE repo_id='%s'" % repo_id
        record = session.execute(text(sql)).fetchone()

    if not record or not record[0]:
        return False

    return True


def is_wiki_page(path):
    if path.split('/')[1] in WIKI_DIRS and path.endswith('.sdoc'):
        return True
    return False


def extract_sdoc_text(content):
    data = json.loads(content)
    texts = []
    def extract_text(node):
        if isinstance(node, dict):
            if "text" in node:
                texts.append(node["text"])
            for key, value in node.items():
                extract_text(value)
        elif isinstance(node, list):
            for item in node:
                extract_text(item)
    extract_text(data)
    result = ' '.join(texts)
    return result
