import logging
from milvus_benchmark.env.base import BaseEnv, ClientEnv

logger = logging.getLogger("milvus_benchmark.env.docker")


class DockerEnv(BaseEnv):
    """docker env class wrapper"""
    env_mode = "docker"

    def __init__(self, deploy_mode=None):
        super(DockerEnv, self).__init__(deploy_mode)


class DockerClientEnv(ClientEnv):
    """docstring for Client Env"""
    def __init__(self, client_deploy_mode="docker", sdk_version=None):
        super(DockerClientEnv, self).__init__(client_deploy_mode, sdk_version)

    def start_up(self):
        pass

    def tear_down(self):
        pass

    def restart(self):
        pass

    def resources(self):
        pass