import logging
import json


# 发布资料库更新事件到消息队列（Redis）。
def RepoUpdatePublishHandler(config, redis_connection, msg):
    # 检查是否启用了事件发布
    enabled = False
    if config.has_option('EVENTS PUBLISH', 'enabled'):
        enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
    if not enabled:
        return

    # 消息队列类型是否是Redis
    mq_type = ''
    if config.has_option('EVENTS PUBLISH', 'mq_type'):
        mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
    if mq_type != 'REDIS':
        logging.warning("Unknown database backend: %s" % mq_type)
        return

    # 它尝试将消息内容解析为JSON
    try:
        elements = json.loads(msg['content'])
    except:
        logging.warning("got bad message: %s", msg)
        return

    if len(elements.keys()) != 3:
        logging.warning("got bad message: %s", msg)
        return

    # 将其发布到'repo_update'频道：如果消息发布成功，它记录一个调试日志；否则，它记录一个错误日志或信息日志，具体取决于结果。
    try:
        if redis_connection.publish('repo_update', msg['content']) > 0:
            logging.debug('Publish event: %s' % msg['content'])
        else:
            logging.info('No one subscribed to repo_update channel, event (%s) has not been send' % msg['content'])
    except Exception as e:
        logging.error(e)
        logging.error("Failed to publish event: %s " % msg['content'])


# 这个函数注册事件处理函数
def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdatePublishHandler)
