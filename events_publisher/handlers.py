import logging

from seafevents.app.config import appconfig
from seafevents.events_publisher.events_publisher import events_publisher

def RepoUpdatePublishHandler(session, msg):
    if not appconfig.publish_enabled:
        return

    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    events_publisher.publish_event("%s\t%s" % (msg.body, msg.ctime))


def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdatePublishHandler)
