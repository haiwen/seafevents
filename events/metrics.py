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


### metrics decorator
def seasearch_index_timing_decorator(func):
    def wrapper(*args, **kwargs):
        redis_client = args[4]
        publish_metric = {
            "metric_name": "seasearch_index_timing",
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
        if ENABLE_METRIC:
            redis_client.publish("metric-channel", json.dumps(publish_metric))
    return wrapper


def format_metrics(cache):
    metrics = cache.get(REDIS_METRIC_KEY)
    if not metrics:
        return ''
    metrics = json.loads(metrics)

    metric_info = ''
    for metric_name, metric_detail in metrics.items():
        metric_value = metric_detail.pop('metric_value')
        if metric_detail:
            for label_name, label_value in metric_detail.items():
                label = label_name + '="' + str(label_value) + '",'
            label = label[:-1]
            metric_info += metric_name + '{' + label + '} ' + str(metric_value) +'\n'
        else:
            metric_info += metric_name + str(metric_value) + '\n'

    cache.delete("metrics")
    return metric_info.encode()


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
        subscriber = self._redis_client.get_subscriber('metric-channel')

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    metric_data = json.loads(message['data'])
                    try:
                        key_name = metric_data.get('instance_name') + ':' + metric_data.get('node_name') + ':' + metric_data.get('metric_name')
                        metric_details = metric_data.get('details', {})
                        metric_details['metric_value'] = metric_data.get('metric_value')
                        # global
                        local_metric['metrics'][key_name] = metric_details
                    except Exception as e:
                        logging.error('Handle metrics failed: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logging.error('Failed handle metrics: %s' % e)
                subscriber = self._redis_client.get_subscriber('metric-channel')


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

