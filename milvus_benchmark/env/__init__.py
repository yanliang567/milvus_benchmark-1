import logging
from milvus_benchmark import utils

logger = logging.getLogger("milvus_benchmark.env")


class Env(object):
    """docstring for Env"""
    def __init__(self, deploy_mode):
        self.deploy_mode = deploy_mode
        self.env_name = utils.get_unique_name()

    def start_up(self):
        pass

    def tear_down(self):
        pass

    def restart(self):
        pass

    def resources(self):
        pass