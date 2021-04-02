import logging.config
from datetime import datetime
import os
import yaml
import config
from milvus_benchmark import redis_conn

cur_path = os.path.abspath(os.path.dirname(__file__))
LOG_CONFIG_PATH = cur_path + "/logging.yaml"
FILE_NAME = config.LOG_PATH + 'benchmark-{:%Y-%m-%d}.log'.format(datetime.now())


def setup_logging(config_path=LOG_CONFIG_PATH, default_level=logging.INFO):
    """
    Setup logging configuration
    """
    try:
        with open(config_path, 'rt') as f:
            log_config = yaml.safe_load(f.read())
        log_config["handlers"]["info_file_handler"].update({"filename": FILE_NAME})
        logging.config.dictConfig(log_config)
    except Exception:
        raise


class RedisLoggingHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.key = kwargs.get("key")
        super(RedisLoggingHandler, self).__init__()

    def emit(self, record):
        record = datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - "+str(self.format(record))
        redis_conn.rpush(self.key, record)
        redis_conn.expire(self.key, config.REDIS_LOG_EXPIRE_TIME)


