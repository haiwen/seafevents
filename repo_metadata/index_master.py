import time
import logging
from threading import Thread

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env

logger = logging.getLogger(__name__)


class RepoMetadataIndexMaster(Thread):
    """ Publish the news of the events obtained from ccnet 
    """
    def __init__(self, config):
        Thread.__init__(self)

        self.mq_server = '127.0.0.1'
        self.mq_port = 6379
        self.mq_password = ''

        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)

    def _parse_config(self, config):
        section_name = 'REDIS'
        key_server = 'server'
        key_port = 'port'
        key_password = 'password'

        if not config.has_section(section_name):
            return

        self.mq_server = get_opt_from_conf_or_env(config, section_name, key_server, default='')
        self.mq_port = get_opt_from_conf_or_env(config, section_name, key_port, default=6379)
        self.mq_password = get_opt_from_conf_or_env(config, section_name, key_password, default='')

    def run(self):
        logger.info('metadata master starting work')
        while True:
            try:
                self.master_handler()
            except Exception as e:
                logger.error('Error handing master task: %s' % e)
                #prevent waste resource if redis or others didn't connectioned
                time.sleep(0.2)

    def master_handler(self):
        p = self.mq.pubsub(ignore_subscribe_messages=True)
        try:
            p.subscribe('metadata_update')
        except Exception as e:
            logger.error('The connection to the redis server failed: %s' % e)
        else:
            logger.info('metadata master starting listen')
        while True:
            message = p.get_message()
            if message is not None and isinstance(message['data'], str) and message['data'].count('\t') == 2:
                self.mq.lpush('metadata_task', message['data'])
                logger.info('%s has been add to metadata task queue' % message['data'])

            if message is None:
                # prevent waste resource when no message has been send
                time.sleep(5)
