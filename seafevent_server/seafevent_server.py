from threading import Thread
from waitress import serve

from seafevents.seafevent_server.request_handler import app as application
from seafevents.seafevent_server.task_manager import task_manager
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seafevents.seasearch.index_task.index_task_manager import index_task_manager
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager


# SeafEventServer 类是 Thread 的子类，表示一个用于处理 Seafile 事件的服务器。它解析配置设置，初始化任务管理器，并启动一个 WSGI 服务器。
class SeafEventServer(Thread):

    # 使用给定的应用程序和配置初始化服务器。它解析配置，初始化任务管理器，并启动服务器。
    def __init__(self, app, config):
        Thread.__init__(self)
        self._parse_config(config)
        self.app = app
        task_manager.init(self.app, self._workers, self._task_expire_time, config)
        event_export_task_manager.init(self.app, self._workers, self._task_expire_time, config)

        task_manager.run()
        event_export_task_manager.run()
        application.face_recognition_manager = FaceRecognitionManager(config)

        index_task_manager.init(config)

    #  解析服务器的配置设置，包括主机、端口、工作线程数和任务过期时间。如果配置中没有这些设置，它会设置默认值。
    def _parse_config(self, config):
        if config.has_option('SEAF-EVENT-SERVER', 'host'):
            self._host = config.get('SEAF-EVENT-SERVER', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('SEAF-EVENT-SERVER', 'port'):
            self._port = config.getint('SEAF-EVENT-SERVER', 'port')
        else:
            self._port = '8889'

        if config.has_option('SEAF-EVENT-SERVER', 'workers'):
            self._workers = config.getint('SEAF-EVENT-SERVER', 'workers')
        else:
            self._workers = 3

        if config.has_option('SEAF-EVENT-SERVER', 'task_expire_time'):
            self._task_expire_time = config.getint('SEAF-EVENT-SERVER', 'task_expire_time')
        else:
            self._task_expire_time = 30 * 60

    # 启动 WSGI 服务器并使其一直运行。
    def run(self):
        serve(application, host=self._host, port=self._port)


# WSGI（Web 服务器网关接口）服务器是一种用于托管 Python Web 应用程序的服务器。
# 它是一种标准接口，允许 Web 服务器与 Python Web 应用程序框架（如 Django、Flask 等）之间进行通信。

# WSGI 服务器的主要功能是：
# 1. 接收来自 Web 服务器的 HTTP 请求
# 2. 将请求传递给 Python Web 应用程序框架
# 3. 接收来自 Python Web 应用程序框架的响应
# 4. 将响应发送回 Web 服务器

# WSGI 服务器通常提供以下功能：
# * 请求/响应处理
# * URL 路由
# * 中间件支持
# * 静态文件服务

# 常见的 WSGI 服务器包括：
# * Gunicorn
# * uWSGI
# * mod_wsgi（Apache 服务器的 WSGI 模块）
# * WSGIServer（Python 的内置 WSGI 服务器）

# 在上面的代码中，`WSGIServer` 是一个 WSGI 服务器实例，它托管了 `application` 对象，这是一个 Python Web 应用程序框架。