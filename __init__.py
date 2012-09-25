"""
Event: General user event class, has these attributes:
    - username
    - timestamp
    - etype
    - <other event-specific attributes ...> , see the table below

----------------------------------

event details:

|-------------+---------------------------+-----------------------------|
| etype       | type specific attributes  | more info                   |
|-------------+---------------------------+-----------------------------|
|-------------+---------------------------+-----------------------------|
| repo-update | repo_id, commit_id        |                             |
| repo-create | owner, repo_id, repo_name |                             |
| repo-delete | owner, repo_id, repo_name |                             |
| repo-share  | type, from, to, repo_id   | type can be 'group', 'user' |
|-------------+---------------------------+-----------------------------|
| join-group  | user, group               |                             |
|-------------+---------------------------+-----------------------------|
| quit-group  | user, group               |                             |
|-------------+---------------------------+-----------------------------|
    
"""

from db import init_db_session
from db import get_user_events, save_user_events
