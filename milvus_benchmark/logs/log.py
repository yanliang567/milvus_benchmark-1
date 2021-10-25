import logging.config
from datetime import datetime
import os
import yaml
import config
import milvus_benchmark.utils as utils

cur_path = os.path.abspath(os.path.dirname(__file__))
LOG_CONFIG_PATH = cur_path + "/logging.yaml"
FILE_NAME = config.LOG_PATH + 'benchmark-{:%Y-%m-%d}.log'.format(datetime.now())


def setup_logging(config_path=LOG_CONFIG_PATH, default_level=logging.INFO):
    """
    Setup logging configuration
    """

    print("log_path.log_file_path: " + str(global_params.log_file_path))
    print("log_path.locust_report_path: " + str(global_params.locust_report_path))
    try:
        with open(config_path, 'rt') as f:
            log_config = yaml.safe_load(f.read())

        log_config["handlers"]["info_file_handler"].update({"filename": global_params.log_file_path})

        # utils.modify_file([log_path.locust_report_path], is_modify=True)
        log_config["handlers"]["locust_file_handler"].update({"filename": global_params.locust_report_path})

        logging.config.dictConfig(log_config)
    except Exception:
        raise logging.error('Failed to open file', exc_info=True)


class GlobalParams:
    log_file_path = FILE_NAME
    locust_report_path = '/tmp/logs/locust_report.log'
    config_path = "/test/milvus/config/config.json"
    # config_path = "/tmp/config.json"
    metric = None


global_params = GlobalParams()
