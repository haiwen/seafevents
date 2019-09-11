# coding: utf-8
import logging
import logging.handlers

from datetime import datetime
from .counter import update_hash_record, save_traffic_info


def UserLoginEventHandler(session, msg):
    elements = msg['content'].split('\t')
    if len(elements) != 4:
        logging.warning("got bad message: %s", elements)
        return
    username = elements[1]
    timestamp = elements[2]
    org_id = elements[3]
    _timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

    update_hash_record(session, username, _timestamp, org_id)


def FileStatsEventHandler(session, msg):
    elements = msg['content'].split('\t')
    if len(elements) != 4:
        logging.warning("got bad message: %s", elements)
        return

    timestamp = datetime.utcfromtimestamp(msg['ctime'])
    oper = elements[0]
    user_name = elements[1]
    repo_id = elements[2]
    size = int(elements[3])

    save_traffic_info(session, timestamp, user_name, repo_id, oper, size)


def register_handlers(handlers):
    handlers.add_handler('seahub.stats:user-login', UserLoginEventHandler)
    handlers.add_handler('seaf_server.stats:web-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:web-file-download', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:link-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:link-file-download', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:sync-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:sync-file-download', FileStatsEventHandler)
