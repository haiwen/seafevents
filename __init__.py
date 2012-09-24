"""
Event: General event class
    - uuid
    - timestamp
    - etype
    - detail: json format (see below for keys)

----------------------------------

event details:

|-------------+--------------------|
| etype       | detail keys        |
|-------------+--------------------|
|-------------+--------------------|
| repo-update | repo_id, commit_id |
|-------------+--------------------|
    
"""

from db import init_db_session
from db import get_user_events