# coding: utf-8
import logging
import json
import logging.handlers

from datetime import datetime
from .counter import update_hash_record, save_traffic_info

# statistics/handlers.py 定义了几个处理统计数据的事件处理函数。
# 这些函数似乎是用于处理来自其他模块或服务的事件消息，并将这些事件数据存储到数据库中。
# 文件顶部的代码片段中，UserLoginEventHandler 和 FileStatsEventHandler 函数都检查了配置文件中的 "STATISTICS" 部分是否启用
# 如果启用，则处理事件数据并将其存储到数据库中。

# UserLoginEventHandler：处理用户登录事件
def UserLoginEventHandler(config, session, msg):

    # 检查配置，则处理事件数据并将其存储到数据库中。
    enabled = False
    if config.has_option('STATISTICS', 'enabled'):
        enabled = config.getboolean('STATISTICS', 'enabled')
    if not enabled:
        logging.info('statistics is disabled')
        return

    # 从信息中获取用户名称、时间戳和组织ID
    try:
        elements = json.loads(msg['content'])
    except:
        logging.warning("got bad message: %s", msg)
        return

    username = elements.get('user_name')
    timestamp = elements.get('timestamp')
    org_id = elements.get('org_id')
    _timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

    # 更新到数据库
    update_hash_record(session, username, _timestamp, org_id)


# FileStatsEventHandler：处理文件操作事件（如上传、下载等）
def FileStatsEventHandler(config, session, msg):

    # 检查配置，则处理事件数据并将其存储到数据库中
    enabled = False
    if config.has_option('STATISTICS', 'enabled'):
        enabled = config.getboolean('STATISTICS', 'enabled')
    if not enabled:
        logging.info('statistics is disabled')
        return

    try:
        elements = json.loads(msg['content'])
    except:
        logging.warning("got bad message: %s", msg)
        return

    # 保存文件操作到数据库
    timestamp = datetime.utcfromtimestamp(msg['ctime'])
    oper = elements.get('msg_type')
    user_name = elements.get('user_name')
    repo_id = elements.get('repo_id')
    size = int(elements.get('bytes'))

    save_traffic_info(session, timestamp, user_name, repo_id, oper, size)


# register_handlers 函数，用于注册这些事件处理函数。
def register_handlers(handlers):
    # 用户登录，触发第一个事件处理函数
    handlers.add_handler('seahub.stats:user-login', UserLoginEventHandler)
    # 文件操作，触发第二个事件处理函数（普通文件上传下载，链接文件上传下载，同步文件上传下载）
    handlers.add_handler('seaf_server.stats:web-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:web-file-download', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:link-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:link-file-download', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:sync-file-upload', FileStatsEventHandler)
    handlers.add_handler('seaf_server.stats:sync-file-download', FileStatsEventHandler)
