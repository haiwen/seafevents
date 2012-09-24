"""
Event: General event class
    - uuid
    - timestamp
    - etype
    - detail: json format (see below for keys)

----------------------------------

event details:

|-------------+---------------------------+-----------------------------|
| etype       | detail keys               | more info                   |
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
from db import get_user_events
