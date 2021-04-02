from redis import StrictRedis, ConnectionPool
from . import config

pool = ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
redis_conn = StrictRedis(connection_pool=pool)