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

from .events.db import init_db_session_class
from .events.db import get_user_events, get_org_user_events, delete_event
from .events.db import save_user_events, save_org_user_events

from .utils import get_office_converter_conf, get_seafes_conf

def is_office_converter_enabled(config):
    conf = get_office_converter_conf(config)
    return conf['enabled']

def is_search_enabled(config):
    conf = get_seafes_conf(config)
    return conf['enabled']

def get_office_converter_html_dir(config):
    conf = get_office_converter_conf(config)
    if not conf['enabled']:
        raise RuntimeError('office conveter is not enabled')

    return os.path.join(conf['outputdir'], 'html')