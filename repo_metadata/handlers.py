import logging
import json
from seafevents.app.config import ENABLE_METADATA_MANAGEMENT


# 资料库元数据更新回调函数（通过 redis 发消息）
def RepoMetadataUpdateHandler(config, redis_connection, msg):
    if not ENABLE_METADATA_MANAGEMENT:
        return

    content = json.loads(msg['content'])
    msg_type = content.get('msg_type')
    repo_id = content.get('repo_id')
    commit_id = content.get('commit_id')

    # 消息错误提示
    if not msg_type or not repo_id or not commit_id:
        logging.warning("got bad message: %s", msg)
        return

    try:
        # 发布消息
        if redis_connection.publish('metadata_update', msg['content']) > 0:
            logging.debug('Publish event: %s' % msg['content'])
        else:
            logging.info('No one subscribed to metadata_update channel, event (%s) has not been send' % msg['content'])
    except Exception as e:
        logging.error(e)
        logging.error("Failed to publish metadata_update event: %s " % msg['content'])


# 注册事件
def register_handlers(handlers):
    handlers.add_handler('seaf_server.event:repo-update', RepoMetadataUpdateHandler)
