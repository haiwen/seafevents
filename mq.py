import redis
import logging

logger = logging.getLogger(__name__)


def get_mq(server, port, password):
    rdp = redis.ConnectionPool(host=server, port=port,
                               password=password, retry_on_timeout=True, decode_responses=True)
    mq = redis.StrictRedis(connection_pool=rdp)
    try:
        mq.ping()
    except Exception as e:
        logger.error("Redis server can't be connected: host %s, port %s, error %s",
                     server, port, e)
    finally:
        # python redis is a client, each operation tries to connect and retry exec
        return mq
