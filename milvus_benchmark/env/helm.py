import os
import logging
import traceback

from milvus_benchmark.env import Env
from milvus_benchmark.env import utils
import helm_utils

logger = logging.getLogger("milvus_benchmark.env.helm")


class HelmEnv(Env):
    """helm env class wrapper"""
    def __init__(self, deploy_mode):
        super(HelmEnv, self).__init__(deploy_mode)
        self.namespace = None

    def start_up(self, helm_path, **helm_install_params):
        self.namespace = helm_install_params["namespace"]
        server_name = helm_install_params["server_name"]
        server_tag = helm_install_params["server_tag"] if "server_tag" in helm_install_params else None
        server_config = helm_install_params["server_config"]
        milvus_config = helm_install_params["milvus_config"]
        if server_name:
            cpus = 32
            try:
                cpus = helm_utils.get_host_cpus(server_name)
            except Exception as e:
                logger.error("Get cpus on host: {} failed".format(server_name))
                logger.error(str(e))
            if server_config:
                if "cpus" in server_config.keys():
                    cpus = min(server_config["cpus"], int(cpus))
            # self.hardware = Hardware(name=self.hostname, cpus=cpus)
        if server_tag:
            cpus = int(server_tag.split("c")[0])
        server_config.update({"cpus": cpus})
        # update values
        values_file_path = helm_path + "/values.yaml"
        if not os.path.exists(values_file_path):
            raise Exception("File {} not existed".format(values_file_path))
        if milvus_config:
            helm_utils.update_values(values_file_path, deploy_mode, server_host, server_tag, milvus_config, server_config)
            logger.debug("Config file has been updated")
        try:
            logger.debug("Start install server")
            host = helm_utils.helm_install_server(helm_path, deploy_mode, image_tag, image_type, self.env_name,
                                                       self.namespace)
            if not host:
                logger.error("Helm install server failed")
                self.clean_up()
                return False
            else:
                return host
        except Exception as e:
            logger.error("Helm install server failed: %s" % (str(e)))
            logger.error(traceback.format_exc())
            self.clean_up()
            return False

    def tear_down(self):
        logger.debug("Start clean up: {}.{}".format(self.env_name, self.namespace))
        helm_utils.helm_del_server(self.env_name, self.namespace)