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
import ConfigParser

from .events.db import init_db_session_class
from .events.db import get_user_events, get_org_user_events, delete_event
from .events.db import save_user_events, save_org_user_events

from .utils import has_office_tools
from .utils.config import get_office_converter_conf
from .tasks import IndexUpdater

def is_search_enabled(config):
    index_updater = IndexUpdater(config)
    return index_updater.is_enabled()

def is_office_converter_enabled(config):
    if not has_office_tools():
        return False

    # TODO: office converter is disabled currently in cluster mode
    def is_cluster_enabled():
        cp = ConfigParser.ConfigParser()
        seafile_conf = os.path.join(os.environ['SEAFILE_CONF_DIR'], 'seafile.conf')
        cp.read(seafile_conf)
        section = 'cluster'
        if not cp.has_section(section):
            return False
        try:
            return cp.getboolean(section, 'enabled')
        except ConfigParser.NoOptionError:
            return False

    if is_cluster_enabled():
        return False

    conf = get_office_converter_conf(config)

    return conf.get('enabled', False)

def get_office_converter_html_dir(config):
    if not has_office_tools():
        raise RuntimeError('office converter is not enabled')

    conf = get_office_converter_conf(config)
    if not conf['enabled']:
        raise RuntimeError('office conveter is not enabled')

    return os.path.join(conf['outputdir'], 'html')

def get_office_converter_limit(config):
    if not has_office_tools():
        raise RuntimeError('office converter is not enabled')

    conf = get_office_converter_conf(config)
    if not conf['enabled']:
        raise RuntimeError('office conveter is not enabled')

    max_size = conf['max_size']
    max_pages = conf['max_pages']
    return max_size, max_pages