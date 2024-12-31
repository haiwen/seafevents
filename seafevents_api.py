from .db import init_db_session_class
from .statistics.db import *
from .events.db import *
from .events.handlers import get_delete_records
from .content_scanner.db import *
from .virus_scanner.db_oper import *
from .app.config import is_repo_auto_del_enabled, is_search_enabled, is_audit_enabled, \
    is_seasearch_enabled
from seafevents.metrics import registry, history_func_decorator
from prometheus_client import exposition


def is_pro():
    return False


@history_func_decorator('get_file_history_suffix')
def get_file_history_suffix(config):
    fh_enabled = True
    if config.has_option('FILE HISTORY', 'enabled'):
        fh_enabled = config.getboolean('FILE HISTORY', 'enabled')
    if fh_enabled is False:
        return []

    suffix = 'md,txt,doc,docx,xls,xlsx,ppt,pptx'
    if config.has_option('FILE HISTORY', 'suffix'):
        suffix = config.get('FILE HISTORY', 'suffix')
    fh_suffix_list = suffix.strip(',').split(',') if suffix else []
    return fh_suffix_list


def get_metrics():
    return exposition.generate_latest(registry)
