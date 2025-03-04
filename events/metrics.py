import os
import json
import time
import datetime
import logging
from threading import Thread, Event
from seafevents.app.config import ENABLE_METRIC
from seafevents.app.event_redis import redis_cache, RedisClient, REDIS_METRIC_KEY


local_metric = {'metrics': {}}

NODE_NAME = os.environ.get('NODE_NAME', 'default')
METRIC_CHANNEL_NAME = "metric-channel"

### metrics decorator
def handle_metric_timing(metric_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not ENABLE_METRIC:
                return func(*args, **kwargs)
            publish_metric = {
                "metric_name": metric_name,
                "instance_name": "seafevents",
                "node_name": NODE_NAME,
                "details": {
                    "collected_at": datetime.datetime.now().isoformat()
                }
            }
            start_time = time.time()
            func(*args, **kwargs)
            end_time = time.time()
            duration_seconds = end_time - start_time
            publish_metric['metric_value'] = round(duration_seconds, 3)
            redis_cache.publish(METRIC_CHANNEL_NAME, json.dumps(publish_metric))
        return wrapper
    return decorator


class MetricHandler(object):
    def __init__(self, app, config):

        self.app = app
        self.config = config

    def start(self):
        MetricTask(self.app, self.config).start()


class MetricTask(Thread):
    def __init__(self, app, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient(config)
        self.app = app

    def run(self):
        logging.info('Starting handle redis channel')
        subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    metric_data = json.loads(message['data'])
                    try:
                        instance_name = metric_data.get('instance_name')
                        node_name = metric_data.get('node_name', 'default')
                        metric_name = metric_data.get('metric_name')
                        key_name = '%s:%s:%s' % (instance_name, node_name, metric_name)
                        metric_details = metric_data.get('details') or {}
                        metric_details['metric_value'] = metric_data.get('metric_value')
                        # global
                        local_metric['metrics'][key_name] = metric_details
                    except Exception as e:
                        logging.error('Handle metrics failed: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logging.error('Failed handle metrics: %s' % e)
                subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)


class MetricRedisRecorder(object):

    def __init__(self):
        self._interval = 15

    def start(self):
        logging.info('Start metric collect, interval = %s sec', self._interval)
        MetricRedisCollect(self._interval).start()


class MetricRedisCollect(Thread):

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
        self.finished = Event()

    def run(self):

        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    if local_metric.get('metrics'):
                        redis_cache.create_or_update(REDIS_METRIC_KEY, local_metric.get('metrics'))
                        local_metric['metrics'].clear()
                except Exception as e:
                    logging.exception('metric collect error: %s', e)

    def cancel(self):
        self.finished.set()


class MetricsManager(object):
    def __init__(self, app=None, config=None):
        self.app = app
        self.config = config
        self._interval = 15
        
    def start(self):
        logging.info('Start metric collect, interval = %s sec', self._interval)
        self._metric_collect_thread = MetricRedisCollect(self._interval)
        self._metric_collect_thread.start()
        
        if self.app and self.config:
            logging.info('Starting metric handler')
            self._metric_task = MetricTask(self.app, self.config)
            self._metric_task.start()
