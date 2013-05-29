# coding: utf-8

import logging

from seaserv import get_related_users_by_repo, get_org_id_by_repo_id, get_related_users_by_org_repo
from db import save_user_events, save_org_user_events

handlers = {}

def set_handler(etype):
    """Decorator used to specify a handler for a event type"""
    def decorate(func):
        assert not handlers.has_key(etype)
        handlers[etype] = func
        return func
    return decorate

def handle_message(session, msg):
    pos = msg.body.find('\t')
    if pos == -1:
        logging.warning("invalid message format: %s", msg)
        return

    etype = msg.body[:pos]
    if not handlers.has_key(etype):
        if etype != 'put-block':
            logging.warning("no handler for event type %s", etype)
        return

    func = handlers[etype]
    try:
        func (session, msg)
    except:
        logging.exception("error when handle msg %s", msg)

@set_handler('repo-update')
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

    if org_id > 0:
        save_org_user_events (session, org_id, etype, detail, users, msg.ctime)
    else:
        save_user_events (session, etype, detail, users, msg.ctime)