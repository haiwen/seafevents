import logging

from .db import init_db_session_class

from .events.db import get_user_events, get_org_user_events, get_user_activities, delete_event, \
        get_file_audit_events, get_file_update_events, get_perm_audit_events, \
        get_event_log_by_time, get_file_audit_events_by_path, save_user_events, \
        save_org_user_events, save_user_activity, get_file_history, get_user_activities_by_timestamp

from .statistics.db import get_file_ops_stats_by_day, get_user_activity_stats_by_day, \
        get_total_storage_stats_by_day, get_org_user_traffic_by_day, \
        get_user_traffic_by_day, get_org_traffic_by_day, get_system_traffic_by_day,\
        get_all_users_traffic_by_month, get_all_orgs_traffic_by_month

from .virus_scanner import get_virus_files, delete_virus_file, operate_virus_file, \
        get_virus_file_by_vid

from .content_scanner.db import get_content_scan_results

from .tasks import IndexUpdater, RepoOldFileAutoDelScanner

logger = logging.getLogger(__name__)


def is_repo_auto_del_enabled(config_file):
    repo_auto_del_scanner = RepoOldFileAutoDelScanner(config_file)
    return repo_auto_del_scanner.is_enabled()


def is_search_enabled(config):
    index_updater = IndexUpdater(config)
    return index_updater.is_enabled()


def is_audit_enabled(config):

    if config.has_section('Audit'):
        audit_section = 'Audit'
    elif config.has_section('AUDIT'):
        audit_section = 'AUDIT'
    else:
        logger.debug('No "AUDIT" section found')
        return False

    enable_audit = False
    if config.has_section(audit_section):
        if config.has_option(audit_section, 'enable'):
            enable_param = 'enable'
        elif config.has_option(audit_section, 'enabled'):
            enable_param = 'enabled'
        else:
            enable_param = None

        if enable_param:
            try:
                enable_audit = config.getboolean(audit_section, enable_param)
            except ValueError:
                pass

    if enable_audit:
        logger.info('audit is enabled')
    else:
        logger.info('audit is not enabled')

    return enable_audit
