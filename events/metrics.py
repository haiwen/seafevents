import os
import json
import time
import datetime
import logging
from threading import Thread, Event
from seafevents.app.config import ENABLE_METRIC
from seafevents.app.event_redis import redis_cache, RedisClient, REDIS_METRIC_KEY, REDIS_METRIC_LABELS_KEY
import hashlib


local_metric = {'metrics': {}}

NODE_NAME = os.environ.get('NODE_NAME', 'default')
METRIC_CHANNEL_NAME = "metric-channel"

# 添加一个用于存储标签映射的Redis key

### metrics decorator
def handle_metric_timing(metric_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not ENABLE_METRIC:
                return func(*args, **kwargs)
            publish_metric = {
                "metric_name": metric_name,
                "metric_type": "gauge",
                "metric_help": "",
                "component_name": "seafevents",
                "node_name": NODE_NAME,
                "labels": {
                    "process": "video_processing",
                    "status": "success"
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


class MetricReceiver(Thread):
    """
    Collect metrics from redis channel and save to local variable
    """
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()

    def run(self):
        logging.info('Starting handle redis channel')
        subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    metric_data = json.loads(message['data'])
                    try:
                        component_name = metric_data.get('component_name')
                        node_name = metric_data.get('node_name', 'default')
                        metric_name = metric_data.get('metric_name')
                        
                        # 1. 使用更细粒度的标识
                        metric_labels = metric_data.get('labels', {})
                        metric_labels.update({
                            'node': node_name,
                            'component': component_name
                        })
                        
                        # 2. 使用MD5生成标签组合的唯一标识
                        # 使用 "|" 作为分隔符，因为它在指标名称中不常见
                        label_str = '|'.join(f"{k}={v}" for k, v in sorted(metric_labels.items()))
                        # 或者直接对完整的标签字符串进行哈希
                        label_hash = hashlib.md5(label_str.encode()).hexdigest()[:8]
                        
                        # 存储标签映射
                        redis_cache.hset(REDIS_METRIC_LABELS_KEY, label_hash, json.dumps(metric_labels))
                        
                        # 3. 构建指标详情
                        metric_details = metric_data.get('details') or {}
                        metric_details.update({
                            'metric_value': metric_data.get('metric_value'),
                            'metric_type': metric_data.get('metric_type'),
                            'metric_help': metric_data.get('metric_help'),
                            'label_hash': label_hash
                        })
                        
                        # 4. 使用 ":" 作为指标名称和哈希值的分隔符
                        key_name = f"{metric_name}:{label_hash}"
                        
                        # 5. 按照指标名称分组存储
                        if metric_name not in local_metric['metrics']:
                            local_metric['metrics'][metric_name] = {}
                        
                        local_metric['metrics'][metric_name][label_hash] = metric_details
                        
                    except Exception as e:
                        logging.error('Handle metrics failed: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logging.error('Failed handle metrics: %s' % e)
                subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)


class MetricSaver(Thread):
    """
    Save metrics to redis
    """

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
                        current_time = datetime.datetime.now().isoformat()
                        metrics_to_save = {}
                        for metric_name, label_groups in local_metric['metrics'].items():
                            for label_hash, metric_detail in label_groups.items():
                                metric_detail['collected_at'] = current_time
                                # 使用 ":" 作为分隔符
                                metrics_to_save[f"{metric_name}:{label_hash}"] = metric_detail
                        
                        redis_cache.create_or_update(REDIS_METRIC_KEY, metrics_to_save)
                        local_metric['metrics'].clear()
                except Exception as e:
                    logging.exception('metric collect error: %s', e)

    def cancel(self):
        self.finished.set()


class MetricsManager(object):
    def __init__(self):
        self._interval = 15
        
    def start(self):
        logging.info('Start metric collect, interval = %s sec', self._interval)
        self._metric_collect_thread = MetricSaver(self._interval)
        self._metric_collect_thread.start()

        logging.info('Starting metric handler')
        self._metric_task = MetricReceiver()
        self._metric_task.start()
