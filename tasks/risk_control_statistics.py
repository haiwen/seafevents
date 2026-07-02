import json
import jwt
import logging
import time
import requests
from threading import Thread, Lock, Event

from seafevents.app.event_redis import RedisClient
from seafevents.app.config import ENABLE_RISK_CONTROL, RISK_CONTROL_SERVER_URL, JWT_PRIVATE_KEY

logger = logging.getLogger(__name__)
CHANNEL = 'risk-control-statistics'


class RiskControlStatisticsSender(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()
        self.stats_lock = Lock()
        self.no_message_check_interval = 5 * 60

    def send_message(self, message):
        api_type = message.get('api_type')
        if not api_type:
            logger.warning('risk control message has no api_type')
            return

        api_path = api_type.replace('_', '-')
        url = RISK_CONTROL_SERVER_URL.rstrip('/') + f'/api/seafile/statistics-{api_path}/'

        try:
            payload = {'exp': int(time.time()) + 300}
            token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
            if isinstance(token, bytes):
                token = token.decode('utf-8')
            headers = {"Authorization": "Token %s" % token}

            requests.post(url, json=message, headers=headers, timeout=10)
        except Exception as e:
            logger.error('Failed to send risk control message %s to %s: %s', message, url, e)

    def run(self):
        if not self._redis_client.connection:
            logger.warning(
                'Can not start ai stats receiver: redis connection is not initialized')
            return
        subscriber = self._redis_client.get_subscriber(CHANNEL)
        message_check_time = time.time()
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    try:
                        message = json.loads(message['data'])
                    except:
                        logger.warning('risk control message invalid')
                        continue
                    logger.debug('risk control message %s', message)
                    try:
                        with self.stats_lock:
                            self.send_message(message)
                    except Exception as e:
                        logger.exception(
                            'save risk control message %s to memory error %s', message, e)
                else:
                    if (time.time() - message_check_time) > self.no_message_check_interval:
                        subscriber = self._redis_client.get_subscriber(CHANNEL)
                        message_check_time = time.time()
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber(CHANNEL)


class RiskControlStatistics:
    def __init__(self):
        self.interval = 300

    def start(self):
        if not ENABLE_RISK_CONTROL or not RISK_CONTROL_SERVER_URL:
            logger.warning(
                'Can not start risk control statistics: risk control is not enabled or server url is not set')
            return
        logger.info('Starts to send statistics to risk-control server')
        self._risk_control_statistics_sender = RiskControlStatisticsSender()
        self._risk_control_statistics_sender.start()
