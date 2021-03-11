import logging
from env import Env
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.runners")


class BasicRunner(object):
    def __init__(self):
        super(K8sRunner, self).__init__()
        self.service_name = utils.get_unique_name()
        self.host = None
        self.port = config.SERVER_PORT_DEFAULT
        self.hostname = None
        self.env_value = None
        # self.hardware = Hardware()
        self.deploy_mode = None

    def set_up(self, deploy_mode, milvus_config, host_config):
        self._env = Env(deploy_mode)
        self._env.start_up()

    def tear_down(self):
        pass
