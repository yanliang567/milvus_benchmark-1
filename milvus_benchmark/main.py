import os
import sys
import time
from datetime import datetime
import pdb
import argparse
import logging
import traceback
from multiprocessing import Process
from queue import Queue
from logging import handlers
from yaml import full_load, dump
from milvus_benchmark.metrics.models.server import Server
from milvus_benchmark.metrics.models.hardware import Hardware
from milvus_benchmark.metrics.models.env import Env

from milvus_benchmark.env import get_env
from milvus_benchmark.runners import get_runner
from milvus_benchmark.metrics import api
from milvus_benchmark import config
from milvus_benchmark import parser
from scheduler import scheduler
from logs import log

log.setup_logging()
logger = logging.getLogger("milvus_benchmark.main")

DEFAULT_IMAGE = "milvusdb/milvus:latest"
LOG_FOLDER = "logs"
NAMESPACE = "milvus"
SERVER_VERSION = "2.0"
q = Queue()

def positive_int(s):
    i = None
    try:
        i = int(s)
    except ValueError:
        pass
    if not i or i < 1:
        raise argparse.ArgumentTypeError("%r is not a positive integer" % s)
    return i


def get_image_tag(image_version):
    return "%s-release" % (image_version)


def shutdown(event):
    logger.info("Check if there is scheduled jobs in scheduler")
    if not scheduler.get_jobs():
        logger.info("No job in scheduler, will shutdown the scheduler")
        scheduler.shutdown(wait=False)


def run_suite(suite, env_mode, deploy_mode, run_type, run_params, env_params=None, helm_path=None, helm_install_params=None):
    metric = api.Metric()
    try:
        metric.set_run_id()
        metric.set_mode(env_mode)
        metric.env = Env()
        metric.hardware = Hardware()
        metric.server = Server(version=SERVER_VERSION, mode=deploy_mode)
        env = get_env(env_mode, deploy_mode)
        if env_mode == "local":
            env.start_up(env_params["host"], env_params["port"])
        elif env_mode == "helm":
            env.start_up(helm_path, helm_install_params)
        metric.update_status(status="DEPLOYE_SUCC")
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        metric.update_status(status="DEPLOYE_FAILED")
    else:
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
    finally:
        # api.save(metric)
        # time.sleep(10)
        env.tear_down()


def main():
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # helm mode with scheduler
    arg_parser.add_argument(
        "--image-version",
        default="",
        help="image version")
    arg_parser.add_argument(
        "--schedule-conf",
        metavar='FILE',
        default='',
        help="load test schedule from FILE")
    arg_parser.add_argument(
        "--deploy-mode",
        default='',
        help="single or cluster")

    # local mode
    arg_parser.add_argument(
        '--local',
        action='store_true',
        help='use local milvus server')
    arg_parser.add_argument(
        '--host',
        help='server host ip param for local mode',
        default='127.0.0.1')
    arg_parser.add_argument(
        '--port',
        help='server port param for local mode',
        default='19530')
    arg_parser.add_argument(
        '--suite',
        metavar='FILE',
        help='load test suite from FILE',
        default='')

    args = arg_parser.parse_args()
    
    if args.schedule_conf:
        if args.local:
            raise Exception("Helm mode with scheduler and other mode are incompatible")
        if not args.image_version:
            raise Exception("Image version not given")
        env_mode = "helm"
        image_version = args.image_version
        deploy_mode = args.deploy_mode
        with open(args.schedule_conf) as f:
            schedule_config = full_load(f)
            f.close()
        helm_path = os.path.join(os.getcwd(), "..//milvus-helm-charts/charts/milvus-ha")
        for item in schedule_config:
            server_host = item["server"] if "server" in item else ""
            server_tag = item["server_tag"] if "server_tag" in item else ""
            suite_params = item["suite_params"]
            for suite_param in suite_params:
                suite_file = "suites/" + suite_param["suite"]
                with open(suite_file) as f:
                    suite_dict = full_load(f)
                    f.close()
                logger.debug(suite_dict)
                run_type, run_params = parser.operations_parser(suite_dict)
                collections = run_params["collections"]
                image_type = suite_param["image_type"]
                image_tag = get_image_tag(image_version)
                for suite in collections:
                    # run test cases
                    milvus_config = suite["milvus"] if "milvus" in suite else None
                    server_config = suite["server"] if "server" in suite else None
                    logger.debug(milvus_config)
                    logger.debug(server_config)
                    helm_install_params = {
                        "server_name": server_host,
                        "server_tag": server_tag,
                        "server_config": server_config,
                        "milvus_config": milvus_config,
                        "image_tag": image_tag,
                        "image_type": image_type
                    }
                    kwargs = {
                        "helm_path": helm_path,
                        "helm_install_params": helm_install_params
                    }
                    job = scheduler.add_job(run_suite, args=[suite, env_mode, deploy_mode, run_type, run_params], kwargs=kwargs)
                    logger.info(job)
                    logger.info(job.id)

    elif args.local:
        # for local mode
        env_params = {
            "host": args.host,
            "port": args.port
        }
        suite_file = args.suite
        with open(suite_file) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)
        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        if len(collections) > 1:
            raise Exception("Multi collections not supported in Local Mode")
        # ensure there is only one case in suite
        suite = collections[0]
        env_mode = "local"
        deploy_mode = None

        # run_suite(suite, env_mode, deploy_mode, run_type, run_params, env_params)
        kwargs = {
            "env_params": env_params
        }
        job = scheduler.add_job(run_suite, args=[suite, env_mode, deploy_mode, run_type, run_params], kwargs=kwargs)
        logger.info(job)
        logger.info(job.id)


if __name__ == "__main__":
    try:
        main()
        from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
        scheduler.add_listener(shutdown, EVENT_JOB_EXECUTED|EVENT_JOB_ERROR)
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.error("Received interruption")
        scheduler.shutdown(wait=False)
        sys.exit(0)
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)
    logger.info("Finshed")