"""
Event: General user event class, has these attributes:
    - username
    - timestamp
    - etype
    - <other event-specific attributes ...> , see the table below

----------------------------------

event details:

|-------------+---------------------------+--------------------|
| etype       | type specific attributes  | more info          |
|-------------+---------------------------+--------------------|
|-------------+---------------------------+--------------------|
| repo-update | repo_id, commit_id        |                    |
| repo-create | owner, repo_id, repo_name |                    |
| repo-delete | owner, repo_id, repo_name |                    |
| repo-share  | type, from, to, repo_id   | not implmented yet |
|-------------+---------------------------+--------------------|
| join-group  | user, group               | not implmented yet |
| quit-group  | user, group               | not implmented yet |
|-------------+---------------------------+--------------------|

"""

import os
import configparser
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

from .utils import has_office_tools
from .utils.config import get_office_converter_conf
from .tasks import IndexUpdater

logger = logging.getLogger(__name__)

def is_search_enabled(config):
    index_updater = IndexUpdater(config)
    return index_updater.is_enabled()

def is_office_converter_enabled(config):
    if not has_office_tools():
        return False

    conf = get_office_converter_conf(config)

    return conf.get('enabled', False)

def get_office_converter_dir(config, file_type):
    if not has_office_tools():
        logger.error('office converter is not enabled, because no office tool')
        raise RuntimeError('office converter is not enabled')

    conf = get_office_converter_conf(config)
    if not conf['enabled']:
        logger.error('office converter is not enabled')
        raise RuntimeError('office conveter is not enabled')

    return os.path.join(conf['outputdir'], file_type)

def get_office_converter_limit(config):
    if not has_office_tools():
        logger.error('office converter is not enabled, because no office tool')
        raise RuntimeError('office converter is not enabled')

    conf = get_office_converter_conf(config)
    if not conf['enabled']:
        logger.error('office converter is not enabled')
        raise RuntimeError('office conveter is not enabled')

    max_size = conf['max_size']
    max_pages = conf['max_pages']
    return max_size, max_pages

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
