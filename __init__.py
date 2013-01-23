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

from db import init_db_session_class
from db import get_user_events, get_org_user_events, delete_event
from db import save_user_events, save_org_user_events