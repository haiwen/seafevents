import logging

from seafevents.app.config import appconfig
from seafevents.events_publisher.events_publisher import events_publisher


def MoreFileOperationsPublishHandler(session, msg):
    if not appconfig.publish_enabled:
        return

    elements = msg.body.split('\t')
    if elements[0] in ('file-download-web', 'file-download-api', 'file-download-share-link'):
        if len(elements) != 6:
            logging.warning("got bad message: %s", elements)
            return

    if elements[0] == 'file-copy':
        if len(elements) != 6:
            logging.warning("got bad message: %s", elements)
            return

    msg.body = 'more-file-ops\t' + msg.body
    events_publisher.publish_more_file_operations(msg.body)


def RepoUpdatePublishHandler(session, msg):
    if not appconfig.publish_enabled:
        return

    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    events_publisher.publish_event(msg.body)


def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdatePublishHandler)

    handlers.add_handler('seahub.audit:file-download-web', MoreFileOperationsPublishHandler)
    handlers.add_handler('seahub.audit:file-download-api', MoreFileOperationsPublishHandler)
    handlers.add_handler('seahub.audit:file-download-share-link', MoreFileOperationsPublishHandler)
    handlers.add_handler('seahub.audit:file-copy', MoreFileOperationsPublishHandler)
