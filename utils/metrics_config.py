from prometheus_client.core import CollectorRegistry

registry = CollectorRegistry()
# class MetricsRegistry:
#     _instance_lock = threading.Lock()
#
#     @classmethod
#     def instance(cls, *args, **kwargs):
#         if not hasattr(MetricsRegistry, "_instance"):
#             with MetricsRegistry._instance_lock:
#                 if not hasattr(MetricsRegistry, "_instance"):
#                     MetricsRegistry._instance = CollectorRegistry()
#         return MetricsRegistry._instance
#
# metrics_registry = MetricsRegistry.instance()

# __all__ = ["metrics_registry"]
