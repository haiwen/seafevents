# coding: utf-8

import logging

from utils import get_related_users_by_repo
from db import save_user_events

__all__ = [
    "handle_message"
]

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
        logging.warning("no handler for event type %s", etype) 
        return

    func = handlers[etype]
    func (session, msg)

@set_handler('repo-update')
def RepoUpdateEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    commit_id = elements[2]

    users =  get_related_users_by_repo(repo_id)

    if not users:
        return
        
    detail = {'repo_id': repo_id,
          'commit_id': commit_id,
    }
    
    save_user_events (session, 'repo-update', detail, users, msg.ctime)
