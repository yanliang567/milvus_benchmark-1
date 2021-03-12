import logging
from milvus_benchmark.env.base import BaseEnv

logger = logging.getLogger("milvus_benchmark.env.local")


class LocalEnv(BaseEnv):
    """docker env class wrapper"""
    env_mode = "local"

    def __init__(self, deploy_mode=None):
        super(LocalEnv, self).__init__(deploy_mode)

    def start_up(self, hostname, port):
        self.set_hostname(hostname)
