# coding: utf-8

import os
import logging
import logging.handlers
import datetime

from seaserv import get_related_users_by_repo, get_org_id_by_repo_id, \
    get_related_users_by_org_repo, get_repo_owner
from .db import save_user_events, save_org_user_events, \
    update_block_download_traffic, update_file_view_traffic, \
    update_file_download_traffic, update_dir_download_traffic

handlers = {}

LOG_ACCESS_INFO = True

_cached_loggers = {}
def get_logger(name, logfile):
    if name in _cached_loggers:
        return _cached_loggers[name]

    logdir = os.path.join(os.environ.get('SEAFSTAT_LOG_DIR', 'logs'), 'stats')
    if not os.path.exists(logdir):
        os.makedirs(logdir)
    logfile = os.path.join(logdir, logfile)
    logger = logging.getLogger(name)
    handler = logging.handlers.TimedRotatingFileHandler(logfile, when='D', interval=1)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    _cached_loggers[name] = logger

    return logger

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

    etype = msg.app + ':' + msg.body[:pos]
    if not handlers.has_key(etype):
        logging.warning("no handler for event type %s", etype)
        return

    func = handlers[etype]
    try:
        func (session, msg)
    except:
        logging.exception("error when handle msg %s", msg)

@set_handler('seaf_server.event:repo-update')
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

    owner = get_repo_owner(repo_id)

    if LOG_ACCESS_INFO:
        repoupdate_logger = get_logger('repo.update', 'repo_update.log')
        repoupdate_logger.info("%s %s" % (repo_id, owner))

@set_handler('seaf_server.event:put-block')
def PutBlockEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 5:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    peer_id = elements[2]
    block_id = elements[3]
    block_size = elements[4]

    owner = get_repo_owner(repo_id)

    if LOG_ACCESS_INFO:
        blockdownload_logger = get_logger('block.download', 'block_download.log')
        blockdownload_logger.info("%s %s %s %s %s" % (repo_id, owner, peer_id, block_id, block_size))

    if owner:
        update_block_download_traffic(session, owner, int(block_size))

@set_handler('seahub.stats:file-view')
def FileViewEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 5:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    shared_by = elements[2]
    file_id = elements[3]
    file_size = elements[4]

    if LOG_ACCESS_INFO:
        fileview_logger = get_logger('file.view', 'file_view.log')
        fileview_logger.info('%s %s %s %s' % (repo_id, shared_by, file_id, file_size))

    file_size = int(file_size)
    if file_size > 0:
        update_file_view_traffic(session, shared_by, int(file_size))

@set_handler('seahub.stats:file-download')
def FileDownloadEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 5:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    shared_by = elements[2]
    file_id = elements[3]
    file_size = elements[4]

    if LOG_ACCESS_INFO:
        filedownload_logger = get_logger('file.download', 'file_download.log')
        filedownload_logger.info('%s %s %s %s' % (repo_id, shared_by, file_id, file_size))

    file_size = int(file_size)
    if file_size > 0:
        update_file_download_traffic(session, shared_by, file_size)

@set_handler('seahub.stats:dir-download')
def DirDownloadEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 5:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    shared_by = elements[2]
    dir_id = elements[3]
    dir_size = elements[4]

    if LOG_ACCESS_INFO:
        dirdownload_logger = get_logger('dir.download', 'dir_download.log')
        dirdownload_logger.info('%s %s %s %s' % (repo_id, shared_by, dir_id, dir_size))

    dir_size = int(dir_size)
    if dir_size > 0:
        update_dir_download_traffic(session, shared_by, dir_size)