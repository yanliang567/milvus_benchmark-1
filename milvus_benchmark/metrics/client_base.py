import logging
logger = logging.getLogger("milvus_benchmark.metric.client_base")


class ClientBase:
    def __init__(self):
        pass

    def query(self, *args, **kwargs):
        logger.debug("[ClientBase] query function %s" % (str(*args) + str(**kwargs)))

    def insert(self, *args, **kwargs):
        logger.debug("[ClientBase] insert function %s" % (str(*args) + str(**kwargs)))
