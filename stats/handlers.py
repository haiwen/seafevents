# coding: utf-8

import os
import logging
import logging.handlers

from seaserv import get_repo_owner
from .db import update_block_download_traffic, update_file_view_traffic, \
    update_file_download_traffic, update_dir_download_traffic

LOG_ACCESS_INFO = False

_cached_loggers = {}
def get_logger(name, logfile):
    if name in _cached_loggers:
        return _cached_loggers[name]

    logdir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'stats-logs')
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

def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:put-block', PutBlockEventHandler)
    handlers.add_handler('seahub.stats:file-view', FileViewEventHandler)
    handlers.add_handler('seahub.stats:file-download', FileDownloadEventHandler)
    handlers.add_handler('seahub.stats:dir-download', DirDownloadEventHandler)
