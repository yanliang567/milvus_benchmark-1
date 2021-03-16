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
from milvus_benchmark.env import get_env
from milvus_benchmark.runners import get_runner
from milvus_benchmark.metrics import api
from milvus_benchmark import config
from milvus_benchmark import parser
from logs import log

log.setup_logging()
logger = logging.getLogger("milvus_benchmark.main")

DEFAULT_IMAGE = "milvusdb/milvus:latest"
LOG_FOLDER = "logs"
NAMESPACE = "milvus"


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


def queue_worker(queue):
    while not queue.empty():
        q = queue.get()
        suite = q["suite"]
        server_host = q["server_host"]
        server_tag = q["server_tag"]
        deploy_mode = q["deploy_mode"]
        image_type = q["image_type"]
        image_tag = q["image_tag"]

        with open(suite) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)

        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        env_mode = "helm"
        helm_path = os.path.join(os.getcwd(), "../milvus-helm-charts/charts/milvus-ha")
        metric = api.Metric()
        for collection in collections:
            # run tests
            milvus_config = collection["milvus"] if "milvus" in collection else None
            server_config = collection["server"] if "server" in collection else None
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
            try:
                metric.set_run_id()
                # metric.env = None
                # metric.hardware = None
                server_version = "2.0"
                # metric.server = Server(version=server_version, mode=deploy_mode)
                metric.run_params = run_params
                env = get_env(env_mode, deploy_mode)
                if not env.start_up(helm_path, helm_install_params):
                    metric.update(status="DEPLOYE_FAILED")
                else:
                    metric.update(status="DEPLOYE_SUCC")
            except Exception as e:
                logger.error(str(e))
                logger.error(traceback.format_exc())
                metric.update(status="DEPLOYE_FAILED")
            else:
                runner = get_runner(run_type, env, metric)
                if runner.run(run_params):
                    metric.update(status="RUN_SUCC")
                    api.save(metric)
                else:
                    logger.error(str(e))
                    logger.error(traceback.format_exc())
            finally:
                time.sleep(10)
                env.stop()
                metric.update(status="CLEAN_SUCC")


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
        image_version = args.image_version
        deploy_mode = args.deploy_mode
        with open(args.schedule_conf) as f:
            schedule_config = full_load(f)
            f.close()
        queues = []
        # server_names = set()
        server_names = []
        for item in schedule_config:
            server_host = item["server"] if "server" in item else ""
            server_tag = item["server_tag"] if "server_tag" in item else ""
            suite_params = item["suite_params"]
            server_names.append(server_host)
            q = Queue()
            for suite_param in suite_params:
                suite = "suites/" + suite_param["suite"]
                image_type = suite_param["image_type"]
                image_tag = get_image_tag(image_version, image_type)
                q.put({
                    "suite": suite,
                    "server_host": server_host,
                    "server_tag": server_tag,
                    "deploy_mode": deploy_mode,
                    "image_tag": image_tag,
                    "image_type": image_type
                })
            queues.append(q)
        logging.error(queues)
        thread_num = len(server_names)
        processes = []

        # debug mode
        queue_worker(queues[0])

        # for i in range(thread_num):
        #     x = Process(target=queue_worker, args=(queues[i], ))
        #     processes.append(x)
        #     x.start()
        #     time.sleep(10)
        # for x in processes:
        #     x.join()

    elif args.local:
        # for local mode
        host = args.host
        port = args.port
        suite_file = args.suite
        with open(suite_file) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)
        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        if len(collections) > 1:
            raise Exception("Multi collections not supported in Local Mode")
        suite = collections[0]
        env_mode = "local"
        deploy_mode = None
        metric = api.Metric()
        try:
            metric.set_run_id()
            metric.set_mode(env_mode)
            # metric.env = None
            # metric.hardware = None
            # server_version = "2.0"
            # metric.server = Server(version=server_version, mode=deploy_mode)
            env = get_env(env_mode, deploy_mode)
            env.start_up(host, port)
            metric.update_status(status="DEPLOYE_SUCC")
        except Exception as e:
            logger.error(str(e))
            logger.error(traceback.format_exc())
            metric.update_status(status="DEPLOYE_FAILED")
        else:
            runner = get_runner(run_type, env, metric)
            cases, case_metrics = runner.extract_cases(suite)
            for index, case in enumerate(cases):
                case_metric = case_metrics[index]
                if runner.run_case(case_metric, **case):
                    case_metric.update_status(status="RUN_SUCC")
                else:
                    case_metric.update_status(status="RUN_FAILED")
                api.save(case_metric)
        finally:
            api.save(metric)
            time.sleep(10)
            env.tear_down()


if __name__ == "__main__":
    main()
    # metric = api.Metric()
    # logger.info(type(metric))
    # api.save(metric)