import time
from functools import wraps
from prometheus_client.metrics import Counter, Gauge
from prometheus_client.core import CollectorRegistry

registry = CollectorRegistry()


### metrics
# task metric
# seafevents_task_metric = Gauge(
#     "seafevents_task_queue_size",
#     f"The total number of task queues.",
#     ['instance', 'taskname'],
#     registry=registry
# )
# def task_metric_decorator(func_name):
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             seafevents_task_metric.labels('seafevents', func_name).inc()
#             result = func(*args, **kwargs)
#             return result
#         return wrapper
#     return decorator

# request metric
seafevents_request_total = Counter(
        'seafevents_request',
        'Total seafevents api request count',
        ['instance', 'request'],
        registry=registry
    )
seafevents_duration_seconds = Gauge(
    'seafevents_duration_seconds',
    'The durations of the currently running requests',
    ['instance', 'func'],
    registry=registry
)
seafevents_duration_seconds_total = Counter(
    'seafevents_request_duration_seconds',
    'Total seafevents request duration',
    ['instance', 'request'],
    registry=registry
)

# history metric
seafevents_history_func_num = Gauge(
    'seafevents_history_func_num',
    'The number of currently running file history requests',
    ['instance', 'func'],
    registry=registry
)
# activity metric
seafevents_file_activity_func_num = Gauge(
    'seafevents_file_activity_func_num',
    'The number of currently running file activity requests',
    ['instance', 'func'],
    registry=registry,
)


def file_activity_func_decorate(func_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            seafevents_file_activity_func_num.labels('seafevents', func_name).inc()
            seafevents_request_total.labels('seafevents', func_name).inc()
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            seafevents_duration_seconds.labels('seafevents', func_name).set(round((end_time - start_time), 3))
            seafevents_duration_seconds_total.labels('seafevents', func_name).inc(round((end_time - start_time), 3))
            seafevents_file_activity_func_num.labels('seafevents', func_name).dec()
            return result
        return wrapper

    return decorator


def history_func_decorator(func_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            seafevents_history_func_num.labels('seafevents', func_name).inc()
            seafevents_request_total.labels('seafevents', func_name).inc()
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            seafevents_duration_seconds.labels('seafevents', func_name).set(round((end_time - start_time), 3))
            seafevents_duration_seconds_total.labels('seafevents', func_name).inc(round((end_time - start_time), 3))
            seafevents_history_func_num.labels('seafevents', func_name).dec()
            return result
        return wrapper

    return decorator
