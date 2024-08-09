import logging
import json


def RepoUpdatePublishHandler(config, redis_connection, msg):
    enabled = False
    if config.has_option('EVENTS PUBLISH', 'enabled'):
        enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
    if not enabled:
        return

    mq_type = ''
    if config.has_option('EVENTS PUBLISH', 'mq_type'):
        mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
    if mq_type != 'REDIS':
        logging.warning("Unknown database backend: %s" % mq_type)
        return

    try:
        elements = json.loads(msg['content'])
    except:
        logging.warning("got bad message: %s", msg)
        return

    if len(elements.keys()) != 3:
        logging.warning("got bad message: %s", msg)
        return

    try:
        if redis_connection.publish('repo_update', msg['content']) > 0:
            logging.debug('Publish event: %s' % msg['content'])
        else:
            logging.info('No one subscribed to repo_update channel, event (%s) has not been send' % msg['content'])
    except Exception as e:
        logging.error(e)
        logging.error("Failed to publish event: %s " % msg['content'])


def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdatePublishHandler)
