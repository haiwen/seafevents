# coding: utf-8

import logging
import simplejson as json

from db import Event, UserEvent
from utils import get_related_users_by_repo

__all__ = [
    "handle_msg"
]

handlers = {}

def handle(etype):
    """Decorator used to specify a handler for a event type"""
    def decorate(func):
        assert not handlers.has_key(etype)
        handlers[etype] = func
        logging.info("add handler for event type %s", etype)
        return func
    return decorate

def handle_msg(session, msg):
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

@handle('repo-update')
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
        
    dt = {'repo_id': repo_id,
          'commit_id': commit_id,
    }
    
    detail = json.dumps(dt)
    event = Event(msg.ctime, 'repo-update', detail)
    session.add(event)
    session.commit()

    for user in users:
        user_event = UserEvent(user, event.uuid)
        session.add(user_event)

    logging.debug("get an event: %s", event)

@handle('repo-create')
def RepoCreateEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 4:
        logging.warning("got bad message: %s", elements)
        return

    creator = elements[0]
    repo_id = elements[1]
    repo_name = elements[2]

    dt = {'repo_name': repo_name,
          'repo_id': repo_id,
    }
    
    detail = json.dumps(dt)
    event = Event(msg.ctime, 'repo-create', detail)
    session.add(event)
    session.commit()

    user_event = UserEvent(creator, event.uuid)
    session.add(user_event)

    logging.debug("get an event: %s", event)
