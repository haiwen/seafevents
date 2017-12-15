import logging

from seafevents.app.config import appconfig

logger = logging.getLogger(__name__)

class EventsPublisher(object):
    def init(self):
        import redis
        if appconfig.publish_mq_type == 'REDIS':
            rdp = redis.ConnectionPool(host=appconfig.publish_mq_server,
                                       port=appconfig.publish_mq_port,
                                       password=appconfig.publish_mq_password,
                                       retry_on_timeout=True)
            self.mq = redis.StrictRedis(connection_pool=rdp)
        try:
            self.mq.ping()
        except:
            logger.error("Redis server can't be connected: host %s, port %s", 
                         appconfig.publish_mq_server, appconfig.publish_mq_port)

    def publish_event(self, event):
    # redis python library already has a connection pool and a retry mechanism
        try:
            if self.mq.publish('repo_update', event) > 0:
                logger.debug('Publish event: %s' % event)
            else:
                logger.info("No one subscribed to repo_update channel, event (%s) has not been send" % event)
        except Exception as e:
            logger.error(e)
            logger.error("Failed to publish event: %s " % event)


events_publisher = EventsPublisher()
