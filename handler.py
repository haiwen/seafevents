# coding: utf-8

import logging

from db import RepoUpdateEvent

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

    event = RepoUpdateEvent(repo_id, commit_id, msg.ctime)

    logging.debug("get an event: %s", event)

    session.add(event)
    session.commit()
