import os
import sys
import time
import pdb
import logging
import traceback
from milvus_benchmark.metrics.models.server import Server
from milvus_benchmark.metrics.models.hardware import Hardware
from milvus_benchmark.metrics.models.env import Env
from milvus_benchmark.logs.log import RedisLoggingHandler

from milvus_benchmark.env import get_env
from milvus_benchmark.runners import get_runner
from milvus_benchmark.metrics import api
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.task")


def run_suite(job_id, suite, env_mode, env_params=None):
    job_logger = RedisLoggingHandler(key=job_id)
    logger.addHandler(job_logger)
    try:
        start_status = False
        metric = api.Metric()
        deploy_mode = env_params["deploy_mode"] if "deploy_mode" in env_params else config.DEFAULT_DEPLOY_MODE
        env = get_env(env_mode, deploy_mode)
        metric.set_run_id()
        metric.set_mode(env_mode)
        metric.env = Env()
        metric.server = Server(version=config.SERVER_VERSION, mode=deploy_mode)
        logger.info(env_params)
        run_type = suite["run_type"]
        if env_mode == "local":
            metric.hardware = Hardware("")
            start_status = env.start_up(env_params["host"], env_params["port"])
        elif env_mode == "helm":
            helm_params = env_params["helm_params"]
            server_name = helm_params["server_name"] if "server_name" in helm_params else None
            server_tag = helm_params["server_tag"] if "server_tag" in helm_params else None
            if not server_name and not server_tag:
                metric.hardware = Hardware("")
            else:
                metric.hardware = Hardware(server_name) if server_name else Hardware(server_tag)
            start_status = env.start_up(helm_params)
        if start_status:
            metric.update_status(status="DEPLOYE_SUCC")
            logger.debug("Get runner")
            runner = get_runner(run_type, env, metric)
            cases, case_metrics = runner.extract_cases(suite)
            # TODO: only run when the as_group is equal to True
            logger.info("Prepare to run cases")
            runner.prepare(**cases[0])
            logger.info("Start run case")
            for index, case in enumerate(cases):
                case_metric = case_metrics[index]
                result = None
                err_message = ""
                try:
                    result = runner.run_case(case_metric, **case)
                except Exception as e:
                    err_message = str(e)+"\n"+traceback.format_exc()
                    logger.error(traceback.format_exc())
                logger.info(result)
                if result:
                    case_metric.update_status(status="RUN_SUCC")
                    case_metric.update_result(result)
                else:
                    case_metric.update_status(status="RUN_FAILED")
                    case_metric.update_message(err_message)
                logger.debug(case_metric.metrics)
                api.save(case_metric)
        else:
            logger.info("Deploy failed on server")
            metric.update_status(status="DEPLOYE_FAILED")
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        metric.update_status(status="DEPLOYE_FAILED")
    finally:
        # api.save(metric)
        # time.sleep(10)
        env.tear_down()