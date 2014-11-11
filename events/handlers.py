# coding: utf-8

import os
import logging
import logging.handlers
import datetime

from seaserv import get_related_users_by_repo, get_org_id_by_repo_id, \
    get_related_users_by_org_repo
from .db import save_user_events, save_org_user_events, save_file_audit_events

def RepoUpdateEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    etype = 'repo-update'
    repo_id = elements[1]
    commit_id = elements[2]

    detail = {'repo_id': repo_id,
          'commit_id': commit_id,
    }

    org_id = get_org_id_by_repo_id(repo_id)
    if org_id > 0:
        users = get_related_users_by_org_repo(org_id, repo_id)
    else:
        users = get_related_users_by_repo(repo_id)

    if not users:
        return

    time = datetime.datetime.utcfromtimestamp(msg.ctime)
    if org_id > 0:
        save_org_user_events (session, org_id, etype, detail, users, time)
    else:
        save_user_events (session, etype, detail, users, time)

def FileAuditEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 7:
        logging.warning("got bad message: %s", elements)
        return

    timestamp = datetime.datetime.utcfromtimestamp(msg.ctime)
    msg_type = elements[0]
    user_name = elements[1]
    ip = elements[2]
    user_agent = elements[3]
    repo_id = elements[4]
    file_path = elements[6].decode('utf-8')

    org_id = get_org_id_by_repo_id(repo_id)

    save_file_audit_events(session, timestamp, msg_type, user_name, ip, \
                           user_agent, org_id, repo_id, file_path)

def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdateEventHandler)
    handlers.add_handler('seahub.stats:file-download-web', FileAuditEventHandler)
    handlers.add_handler('seahub.stats:file-download-api', FileAuditEventHandler)
    handlers.add_handler('seahub.stats:file-download-share-link', FileAuditEventHandler)
