import logging
from seafevents.app.config import ENABLE_METADATA_MANAGEMENT


def RepoMetadataUpdateHandler(config, redis_connection, msg):
    if not ENABLE_METADATA_MANAGEMENT:
        return

    elements = msg['content'].split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    try:
        if redis_connection.publish('metadata_update', msg['content']) > 0:
            logging.debug('Publish event: %s' % msg['content'])
        else:
            logging.info('No one subscribed to metadata_update channel, event (%s) has not been send' % msg['content'])
    except Exception as e:
        logging.error(e)
        logging.error("Failed to publish metadata_update event: %s " % msg['content'])


def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoMetadataUpdateHandler)
