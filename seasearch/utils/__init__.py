import logging
import hashlib

from sqlalchemy import text

from seafevents.seasearch.utils.commit_differ import CommitDiffer

from seafobj import fs_mgr, commit_mgr
from seafobj.exceptions import GetObjectError

from seafevents.repo_metadata.utils import METADATA_TABLE

logger = logging.getLogger(__name__)

SYS_DIRS = ['images', '_Internal']


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
        logger.warning('differ error: %s' % e)
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

    kw = {
        'format': '%(asctime)s [%(levelname)s] %(name)s:%(lineno)s %(funcName)s: %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': level,
        'stream': args.logfile
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


def need_index_summary(repo_id, session, metadata_server_api):
    with session() as session:
        sql = "SELECT enabled FROM repo_metadata WHERE repo_id='%s'" % repo_id
        record = session.execute(text(sql)).fetchone()

    if not record or not record[0]:
        return False

    columns = metadata_server_api.list_columns(repo_id, METADATA_TABLE.id).get('columns', [])
    summary_column = [column for column in columns if column.get('key') == '_description']
    if not summary_column:
        return False

    return True
