import os
import logging
import pdb
import time
import re
import random
import traceback
import json
import csv
import threading
from multiprocessing import Process
import numpy as np
from milvus import DataType
from yaml import full_load, dump
import concurrent.futures

import milvus_benchmark.locust_user
from milvus_benchmark.client import MilvusClient
import milvus_benchmark.parser
from milvus_benchmark.runner import Runner
from milvus_benchmark.metrics.api import report
from milvus_benchmark.metrics.models import Env, Hardware, Server, Metric
from milvus_benchmark.env.helm import HelmEnv
from milvus_benchmark import utils
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.k8s_runner")
DELETE_INTERVAL_TIME = 5
# TODO
INSERT_INTERVAL = 2000
BIG_FLUSH_INTERVAL = 3600
DEFAULT_FLUSH_INTERVAL = 1
timestamp = int(time.time())
default_path = "/var/lib/milvus"


class K8sRunner(Runner):
    """run at k8s mode"""

    def __init__(self):
        super(K8sRunner, self).__init__()
        self.host = None
        self.port = config.SERVER_PORT_DEFAULT
        self.hostname = None
        self.env_value = None
        self.hardware = Hardware()
        self.deploy_mode = None
        self.env = None

    def update_server_config(self, server_name, server_tag, server_config):
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
        return server_config

    def init_env(self, milvus_config, server_config, server_host, server_tag, deploy_mode, image_type, image_tag):
        if server_host:
            logger.debug("Tests run on server host:")
            logger.debug(server_host)
        helm_path = os.path.join(os.getcwd(), "../milvus-helm-charts/charts/milvus-ha")
        self.env = HelmEnv(deploy_mode)
        server_config = self.update_server_config(server_name, server_tag, server_config)
        self.hardware = Hardware(name=self.hostname, cpus=server_config["cpus"])
        helm_install_params = {
            "namespace": config.HELM_NAMESPACE,
            "server_name": service_name,
            "server_tag": server_tag,
            "server_config": server_config,
            "milvus_config": milvus_config,
            "image_tag": image_tag,
            "image_type": image_type
        }
        logger.debug(helm_install_params)
        try:
            self.hostname = env.start_up(helm_path, helm_install_params)
            if self.hostname:
                return True
        except Exception as e:
            logger.error("Helm env: %s start failed".format(env.name))
            return False
        return False

    def clean_up(self):
        logger.debug("Start clean up env: {}".format(self.env.name))
        self.env.tear_down()

    def report_wrapper(self, milvus_instance, env_value, hostname, collection_info, index_info, search_params,
                       run_params=None, server_config=None):
        metric = Metric()
        metric.set_run_id(timestamp)
        metric.env = Env(env_value)
        metric.env.OMP_NUM_THREADS = 0
        metric.hardware = self.hardware
        # TODO: removed
        # server_version = milvus_instance.get_server_version()
        # server_mode = milvus_instance.get_server_mode()
        # commit = milvus_instance.get_server_commit()
        server_version = "2.0"
        server_mode = self.deploy_mode
        metric.server = Server(version=server_version, mode=server_mode, build_commit=None)
        metric.collection = collection_info
        metric.index = index_info
        metric.search = search_params
        metric.run_params = run_params
        return metric

    def run(self, run_type, collection):
        logger.debug(run_type)
        logger.debug(collection)
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        milvus_instance = MilvusClient(collection_name=collection_name, host=self.host)

        # TODO: removed
        # self.env_value = milvus_instance.get_server_config()
        # ugly implemention
        # self.env_value = utils.convert_nested(self.env_value)
        # self.env_value.pop("logs")
        # self.env_value.pop("network")
        # TODO:
        self.env_value = None

        if run_type == "insert_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            if milvus_instance.exists_collection():
                logger.debug("Start drop collection")
                milvus_instance.drop()
                time.sleep(10)
            index_info = {}
            search_params = {}
            vector_type = self.get_vector_type(data_type)
            other_fields = collection["other_fields"] if "other_fields" in collection else None
            milvus_instance.create_collection(dimension, data_type=vector_type,
                                              other_fields=other_fields)
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                index_info = {
                    "index_type": index_type,
                    "index_param": index_param
                }
                index_field_name = utils.get_default_field_name(vector_type)
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
                logger.debug(milvus_instance.describe_index(index_field_name))
            res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            flush_time = 0.0
            if "flush" in collection and collection["flush"] == "no":
                logger.debug("No manual flush")
            else:
                start_time = time.time()
                milvus_instance.flush()
                flush_time = time.time() - start_time
                logger.debug(milvus_instance.count())
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name,
                "other_fields": other_fields,
                "ni_per": ni_per
            }
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         search_params)
            total_time = res["total_time"]
            build_time = 0
            if build_index is True:
                logger.debug("Start build index for last file")
                start_time = time.time()
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
                build_time = time.time() - start_time
                total_time = total_time + build_time
            metric.metrics = {
                "type": run_type,
                "value": {
                    "total_time": total_time,
                    "qps": res["qps"],
                    "ni_time": res["ni_time"],
                    "flush_time": flush_time,
                    "build_time": build_time
                }
            }
            logger.debug(metric.metrics)
            report(metric)

        elif run_type == "build_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            # TODO: enable
            # if not milvus_instance.exists_collection():
            #     logger.error("Table name: %s not existed" % collection_name)
            #     return

            ni_per = collection["ni_per"]
            if milvus_instance.exists_collection():
                logger.debug("Start drop collection")
                milvus_instance.drop()
                time.sleep(10)

            vector_type = self.get_vector_type(data_type)
            other_fields = collection["other_fields"] if "other_fields" in collection else None
            milvus_instance.create_collection(dimension, data_type=vector_type,
                                              other_fields=other_fields)
            self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            milvus_instance.flush()

            search_params = {}
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            start_time = time.time()
            # enable
            # drop index
            # logger.debug("Drop index")
            # milvus_instance.drop_index(index_field_name)

            # start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            # TODO: need to check
            milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            logger.debug(milvus_instance.describe_index(index_field_name))
            logger.debug(milvus_instance.count())
            end_time = time.time()
            # end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         search_params)
            metric.metrics = {
                "type": "build_performance",
                "value": {
                    "build_time": round(end_time - start_time, 1),
                }
            }
            report(metric)

        elif run_type == "delete_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            ni_per = collection["ni_per"]
            auto_flush = collection["auto_flush"] if "auto_flush" in collection else True
            search_params = {}
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                logger.error("Table name: %s not existed" % collection_name)
                return
            length = milvus_instance.count()
            logger.info(length)
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            ids = [i for i in range(length)]
            loops = int(length / ni_per)
            milvus_instance.load_collection()
            # TODO: remove
            # start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_time = time.time()
            # if auto_flush is False:
            #     milvus_instance.set_config("storage", "auto_flush_interval", BIG_FLUSH_INTERVAL)
            for i in range(loops):
                delete_ids = ids[i * ni_per: i * ni_per + ni_per]
                logger.debug("Delete %d - %d" % (delete_ids[0], delete_ids[-1]))
                milvus_instance.delete(delete_ids)
                logger.debug("Table row counts: %d" % milvus_instance.count())
            logger.debug("Table row counts: %d" % milvus_instance.count())
            start_flush_time = time.time()
            milvus_instance.flush()
            end_flush_time = time.time()
            end_time = time.time()
            # end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug("Table row counts: %d" % milvus_instance.count())
            # milvus_instance.set_config("storage", "auto_flush_interval", DEFAULT_FLUSH_INTERVAL)
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         search_params)
            delete_time = round(end_time - start_time, 1)
            metric.metrics = {
                "type": "delete_performance",
                "value": {
                    "delete_time": delete_time,
                    "qps": round(collection_size / delete_time, 1)
                }
            }
            if auto_flush is False:
                flush_time = round(end_flush_time - start_flush_time, 1)
                metric.metrics["value"].update({"flush_time": flush_time})
            report(metric)

        elif run_type == "get_ids_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            ids_length_per_segment = collection["ids_length_per_segment"]
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            search_params = {}
            logger.info(milvus_instance.count())
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            for ids_num in ids_length_per_segment:
                segment_num, get_ids = milvus_instance.get_rand_ids_each_segment(ids_num)
                start_time = time.time()
                get_res = milvus_instance.get_entities(get_ids)
                total_time = time.time() - start_time
                avg_time = total_time / segment_num
                run_params = {"ids_num": ids_num}
                logger.info(
                    "Segment num: %d, ids num per segment: %d, run_time: %f" % (segment_num, ids_num, total_time))
                metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info,
                                             index_info, search_params, run_params=run_params)
                metric.metrics = {
                    "type": run_type,
                    "value": {
                        "total_time": round(total_time, 1),
                        "avg_time": round(avg_time, 1)
                    }
                }
                report(metric)

        elif run_type == "search_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            run_count = collection["run_count"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            # filter_query = collection["filter"] if "filter" in collection else None
            filters = collection["filters"] if "filters" in collection else []
            filter_query = []
            search_params = collection["search_params"]
            # disable
            # if not milvus_instance.exists_collection():
            #     logger.error("Table name: %s not existed" % collection_name)
            #     return
            ni_per = collection["ni_per"]
            if milvus_instance.exists_collection():
                logger.debug("Start drop collection")
                milvus_instance.drop()
                time.sleep(10)

            # need to init data currently
            vector_type = self.get_vector_type(data_type)
            other_fields = collection["other_fields"] if "other_fields" in collection else None
            milvus_instance.create_collection(dimension, data_type=vector_type,
                                              other_fields=other_fields)
            fields = self.get_fields(milvus_instance, collection_name)
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name,
                "fields": fields
            }
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            milvus_instance.flush()
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            start_time = time.time()
            milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)

            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            logger.info(milvus_instance.count())
            logger.info("Start load collection")
            milvus_instance.load_collection()
            logger.info("Start warm up query")
            res = self.do_query(milvus_instance, collection_name, index_field_name, [1], [1], 2,
                                search_param=search_params[0], filter_query=filter_query)
            logger.info("End warm up query")
            for search_param in search_params:
                logger.info("Search param: %s" % json.dumps(search_param))
                if not filters:
                    filters.append(None)
                for filter in filters:
                    filter_param = []
                    if isinstance(filter, dict) and "range" in filter:
                        filter_query.append(eval(filter["range"]))
                        filter_param.append(filter["range"])
                    if isinstance(filter, dict) and "term" in filter:
                        filter_query.append(eval(filter["term"]))
                        filter_param.append(filter["term"])
                    logger.info("filter param: %s" % json.dumps(filter_param))
                    res = self.do_query(milvus_instance, collection_name, index_field_name, top_ks, nqs, run_count,
                                        search_param, filter_query=filter_query)
                    headers = ["Nq/Top-k"]
                    headers.extend([str(top_k) for top_k in top_ks])
                    logger.info("Search param: %s" % json.dumps(search_param))
                    utils.print_table(headers, nqs, res)
                    for index_nq, nq in enumerate(nqs):
                        for index_top_k, top_k in enumerate(top_ks):
                            search_param_group = {
                                "nq": nq,
                                "topk": top_k,
                                "search_param": search_param,
                                "filter": filter_param
                            }
                            search_time = res[index_nq][index_top_k]
                            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname,
                                                         collection_info, index_info, search_param_group)
                            metric.metrics = {
                                "type": "search_performance",
                                "value": {
                                    "search_time": search_time
                                }
                            }
                            report(metric)

        elif run_type == "locust_insert_stress":
            pass

        elif run_type in ["locust_search_performance", "locust_insert_performance", "locust_mix_performance"]:
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            if milvus_instance.exists_collection():
                milvus_instance.drop()
                time.sleep(10)
            index_info = {}
            search_params = {}
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            milvus_instance.create_collection(dimension, data_type=vector_type, other_fields=None)
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                index_info = {
                    "index_type": index_type,
                    "index_param": index_param
                }
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
                logger.debug(milvus_instance.describe_index(index_field_name))
            if run_type in ["locust_search_performance", "locust_mix_performance"]:
                res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
                if "flush" in collection and collection["flush"] == "no":
                    logger.debug("No manual flush")
                else:
                    milvus_instance.flush()
                if build_index is True:
                    logger.debug("Start build index for last file")
                    milvus_instance.create_index(index_field_name, index_type, metric_type, _async=True,
                                                 index_param=index_param)
                    logger.debug(milvus_instance.describe_index(index_field_name))
                logger.debug("Table row counts: %d" % milvus_instance.count())
                milvus_instance.load_collection()
                logger.info("Start warm up query")
                for i in range(2):
                    res = self.do_query(milvus_instance, collection_name, index_field_name, [1], [1], 2,
                                        search_param={"nprobe": 16})
                logger.info("End warm up query")
            real_metric_type = utils.metric_type_trans(metric_type)
            ### spawn locust requests
            task = collection["task"]
            connection_type = "single"
            connection_num = task["connection_num"]
            if connection_num > 1:
                connection_type = "multi"
            clients_num = task["clients_num"]
            hatch_rate = task["hatch_rate"]
            during_time = utils.timestr_to_int(task["during_time"])
            task_types = task["types"]
            run_params = {"tasks": {}, "clients_num": clients_num, "spawn_rate": hatch_rate, "during_time": during_time}
            for task_type in task_types:
                run_params["tasks"].update({task_type["type"]: task_type["weight"] if "weight" in task_type else 1})

            # . collect stats
            locust_stats = locust_user.locust_executor(self.host, self.port, collection_name,
                                                       connection_type=connection_type, run_params=run_params)
            logger.info(locust_stats)
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         search_params)
            metric.metrics = {
                "type": run_type,
                "value": locust_stats}
            report(metric)

        elif run_type == "search_ids_stability":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            search_params = collection["search_params"]
            during_time = collection["during_time"]
            ids_length = collection["ids_length"]
            ids = collection["ids"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            g_top_k = int(collection["top_ks"].split("-")[1])
            l_top_k = int(collection["top_ks"].split("-")[0])
            g_id = int(ids.split("-")[1])
            l_id = int(ids.split("-")[0])
            g_id_length = int(ids_length.split("-")[1])
            l_id_length = int(ids_length.split("-")[0])

            milvus_instance.load_collection()
            # start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            # logger.debug(start_mem_usage)
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                search_param = {}
                top_k = random.randint(l_top_k, g_top_k)
                ids_num = random.randint(l_id_length, g_id_length)
                ids_param = [random.randint(l_id_length, g_id_length) for _ in range(ids_num)]
                for k, v in search_params.items():
                    search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                logger.debug("Query top-k: %d, ids_num: %d, param: %s" % (top_k, ids_num, json.dumps(search_param)))
                result = milvus_instance.query_ids(top_k, ids_param, search_param=search_param)
            # end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         {})
            metric.metrics = {
                "type": "search_ids_stability",
                "value": {
                    "during_time": during_time,
                }
            }
            report(metric)

        # for sift/deep datasets
        # TODO: enable
        elif run_type == "accuracy":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            search_params = collection["search_params"]
            # mapping to search param list
            search_params = self.generate_combinations(search_params)

            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            milvus_instance.load_collection()
            true_ids_all = self.get_groundtruth_ids(collection_size)
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            for search_param in search_params:
                headers = ["Nq/Top-k"]
                res = []
                for nq in nqs:
                    for top_k in top_ks:
                        tmp_res = []
                        search_param_group = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "metric_type": metric_type
                        }
                        logger.info("Query params: %s" % json.dumps(search_param_group))
                        result_ids = self.do_query_ids(milvus_instance, collection_name, index_field_name, top_k, nq,
                                                       search_param=search_param)
                        # mem_used = milvus_instance.get_mem_info()["memory_used"]
                        acc_value = self.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
                        logger.info("Query accuracy: %s" % acc_value)
                        tmp_res.append(acc_value)
                        # logger.info("Memory usage: %s" % mem_used)
                        metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info,
                                                     index_info, search_param_group)
                        metric.metrics = {
                            "type": "accuracy",
                            "value": {
                                "acc": acc_value
                            }
                        }
                        report(metric)
                        # logger.info("Memory usage: %s" % mem_used)
                    res.append(tmp_res)
                headers.extend([str(top_k) for top_k in top_ks])
                logger.info("Search param: %s" % json.dumps(search_param))
                utils.print_table(headers, nqs, res)

        elif run_type == "ann_accuracy":
            hdf5_source_file = collection["source_file"]
            collection_name = collection["collection_name"]
            index_types = collection["index_types"]
            index_params = collection["index_params"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = collection["search_params"]
            # mapping to search param list
            search_params = self.generate_combinations(search_params)
            # mapping to index param list
            index_params = self.generate_combinations(index_params)

            data_type, dimension, metric_type = parser.parse_ann_collection_name(collection_name)
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            dataset = utils.get_dataset(hdf5_source_file)
            if milvus_instance.exists_collection(collection_name):
                logger.info("Re-create collection: %s" % collection_name)
                milvus_instance.drop()
                time.sleep(DELETE_INTERVAL_TIME)
            true_ids = np.array(dataset["neighbors"])
            vector_type = self.get_vector_type_from_metric(metric_type)
            index_field_name = utils.get_default_field_name(vector_type)
            real_metric_type = utils.metric_type_trans(metric_type)
            # re-create collection
            milvus_instance.create_collection(dimension, data_type=vector_type)
            insert_vectors = self.normalize(metric_type, np.array(dataset["train"]))
            if len(insert_vectors) != dataset["train"].shape[0]:
                raise Exception("Row count of insert vectors: %d is not equal to dataset size: %d" % (
                len(insert_vectors), dataset["train"].shape[0]))
            logger.debug("The row count of entities to be inserted: %d" % len(insert_vectors))
            # Insert batch once
            # milvus_instance.insert(insert_vectors)
            loops = len(insert_vectors) // INSERT_INTERVAL + 1
            for i in range(loops):
                start = i * INSERT_INTERVAL
                end = min((i + 1) * INSERT_INTERVAL, len(insert_vectors))
                if start < end:
                    tmp_vectors = insert_vectors[start:end]
                    ids = [i for i in range(start, end)]
                    if not isinstance(tmp_vectors, list):
                        entities = milvus_instance.generate_entities(tmp_vectors.tolist(), ids)
                        res_ids = milvus_instance.insert(entities, ids=ids)
                    else:
                        entities = milvus_instance.generate_entities(tmp_vectors, ids)
                        res_ids = milvus_instance.insert(entities, ids=ids)
                    assert res_ids == ids
            milvus_instance.flush()
            res_count = milvus_instance.count()
            logger.info("Table: %s, row count: %d" % (collection_name, res_count))
            if res_count != len(insert_vectors):
                raise Exception("Table row count is not equal to insert vectors")
            # TODO: not support switch index currently
            for index_type in index_types:
                for index_param in index_params:
                    logger.debug("Building index with param: %s" % json.dumps(index_param))
                    # if milvus_instance.get_config("cluster.enable") == "true":
                    #     milvus_instance.create_index(index_field_name, index_type, metric_type, _async=True,
                    #                                  index_param=index_param)
                    # else:
                    #     milvus_instance.create_index(index_field_name, index_type, metric_type,
                    #                                  index_param=index_param)

                    milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
                    logger.info(milvus_instance.describe_index(index_field_name))
                    logger.info("Start load collection: %s" % collection_name)
                    milvus_instance.load_collection()
                    logger.info("End load collection: %s" % collection_name)
                    index_info = {
                        "index_type": index_type,
                        "index_param": index_param
                    }
                    logger.debug(index_info)
                    warm_up = True
                    for search_param in search_params:
                        for nq in nqs:
                            query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq]))
                            if not isinstance(query_vectors, list):
                                query_vectors = query_vectors.tolist()
                            for top_k in top_ks:
                                search_param_group = {
                                    "nq": len(query_vectors),
                                    "topk": top_k,
                                    "search_param": search_param,
                                    "metric_type": metric_type
                                }
                                logger.debug(search_param_group)
                                vector_query = {"vector": {index_field_name: {
                                    "topk": top_k,
                                    "query": query_vectors,
                                    "metric_type": real_metric_type,
                                    "params": search_param}
                                }}
                                for i in range(2):
                                    result = milvus_instance.query(vector_query)
                                warm_up = False
                                logger.info("End warm up")
                                result = milvus_instance.query(vector_query)
                                result_ids = milvus_instance.get_ids(result)
                                acc_value = self.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
                                logger.info("Query ann_accuracy: %s" % acc_value)
                                metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname,
                                                             collection_info, index_info, search_param_group)
                                metric.metrics = {
                                    "type": "ann_accuracy",
                                    "value": {
                                        "acc": acc_value
                                    }
                                }
                                report(metric)

        elif run_type == "search_stability":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            search_params = collection["search_params"]
            during_time = collection["during_time"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            g_top_k = int(collection["top_ks"].split("-")[1])
            g_nq = int(collection["nqs"].split("-")[1])
            l_top_k = int(collection["top_ks"].split("-")[0])
            l_nq = int(collection["nqs"].split("-")[0])
            milvus_instance.load_collection()
            # start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            # logger.debug(start_mem_usage)
            start_row_count = milvus_instance.count()
            logger.info(start_row_count)
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            real_metric_type = utils.metric_type_trans(metric_type)
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                search_param = {}
                top_k = random.randint(l_top_k, g_top_k)
                nq = random.randint(l_nq, g_nq)
                for k, v in search_params.items():
                    search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                logger.debug("Query nq: %d, top-k: %d, param: %s" % (nq, top_k, json.dumps(search_param)))
                vector_query = {"vector": {index_field_name: {
                    "topk": top_k,
                    "query": query_vectors[:nq],
                    "metric_type": real_metric_type,
                    "params": search_param}
                }}
                milvus_instance.query(vector_query)
            # end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         {})
            metric.metrics = {
                "type": "search_stability",
                "value": {
                    "during_time": during_time,
                }
            }
            report(metric)

        elif run_type == "loop_stability":
            # init data
            milvus_instance.clean_db()
            pull_interval = collection["pull_interval"]
            collection_num = collection["collection_num"]
            concurrent = collection["concurrent"] if "concurrent" in collection else False
            concurrent_num = collection_num
            dimension = collection["dimension"] if "dimension" in collection else 128
            insert_xb = collection["insert_xb"] if "insert_xb" in collection else 100000
            index_types = collection["index_types"] if "index_types" in collection else ['ivf_sq8']
            index_param = {"nlist": 256}
            collection_names = []
            milvus_instances_map = {}
            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
            ids = [i for i in range(insert_xb)]
            # initialize and prepare
            for i in range(collection_num):
                name = utils.get_unique_name(prefix="collection_%d_" % i)
                collection_names.append(name)
                metric_type = random.choice(["l2", "ip"])
                # default float_vector
                milvus_instance = MilvusClient(collection_name=name, host=self.host)
                milvus_instance.create_collection(dimension, other_fields=None)
                index_type = random.choice(index_types)
                field_name = utils.get_default_field_name()
                milvus_instance.create_index(field_name, index_type, metric_type, index_param=index_param)
                logger.info(milvus_instance.describe_index(index_field_name))
                insert_vectors = utils.normalize(metric_type, insert_vectors)
                entities = milvus_instance.generate_entities(insert_vectors, ids)
                res_ids = milvus_instance.insert(entities, ids=ids)
                milvus_instance.flush()
                milvus_instances_map.update({name: milvus_instance})
                logger.info(milvus_instance.describe_index(index_field_name))

                # loop time unit: min -> s
            pull_interval_seconds = pull_interval * 60
            tasks = ["insert_rand", "query_rand", "flush"]
            i = 1
            while True:
                logger.info("Loop time: %d" % i)
                start_time = time.time()
                while time.time() - start_time < pull_interval_seconds:
                    if concurrent:
                        threads = []
                        for name in collection_names:
                            task_name = random.choice(tasks)
                            task_run = getattr(milvus_instances_map[name], task_name)
                            t = threading.Thread(target=task_run, args=())
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()
                        # with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                        #     future_results = {executor.submit(getattr(milvus_instances_map[mp[j][0]], mp[j][1])): j for j in range(concurrent_num)}
                        #     for future in concurrent.futures.as_completed(future_results):
                        #         future.result()
                    else:
                        tmp_collection_name = random.choice(collection_names)
                        task_name = random.choice(tasks)
                        logger.info(tmp_collection_name)
                        logger.info(task_name)
                        task_run = getattr(milvus_instances_map[tmp_collection_name], task_name)
                        task_run()

                logger.debug("Restart server")
                helm_utils.restart_server(self.service_name, namespace)
                # new connection
                # for name in collection_names:
                #     milvus_instance = MilvusClient(collection_name=name, host=self.host)
                #     milvus_instances_map.update({name: milvus_instance})
                time.sleep(30)
                i = i + 1

        elif run_type == "stability":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            during_time = collection["during_time"]
            operations = collection["operations"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                raise Exception("Table name: %s not existed" % collection_name)
            logger.info(milvus_instance.count())
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            index_info = milvus_instance.describe_index(index_field_name)
            logger.info(index_info)
            # start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_row_count = milvus_instance.count()
            logger.info(start_row_count)
            vector_type = self.get_vector_type(data_type)
            real_metric_type = utils.metric_type_trans(metric_type)
            query_vectors = [[random.random() for _ in range(dimension)] for _ in range(10000)]
            if "insert" in operations:
                insert_xb = operations["insert"]["xb"]
            if "delete" in operations:
                delete_xb = operations["delete"]["xb"]
            if "query" in operations:
                g_top_k = int(operations["query"]["top_ks"].split("-")[1])
                l_top_k = int(operations["query"]["top_ks"].split("-")[0])
                g_nq = int(operations["query"]["nqs"].split("-")[1])
                l_nq = int(operations["query"]["nqs"].split("-")[0])
                search_params = operations["query"]["search_params"]
            i = 0
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                i = i + 1
                q = self.gen_executors(operations)
                for name in q:
                    try:
                        if name == "insert":
                            insert_ids = random.sample(list(range(collection_size)), insert_xb)
                            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
                            entities = milvus_instance.generate_entities(insert_vectors, insert_ids)
                            milvus_instance.insert(entities, ids=insert_ids)
                        elif name == "delete":
                            delete_ids = random.sample(list(range(collection_size)), delete_xb)
                            milvus_instance.delete(delete_ids)
                        elif name == "query":
                            top_k = random.randint(l_top_k, g_top_k)
                            nq = random.randint(l_nq, g_nq)
                            search_param = {}
                            for k, v in search_params.items():
                                search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                            logger.debug("Query nq: %d, top-k: %d, param: %s" % (nq, top_k, json.dumps(search_param)))
                            vector_query = {"vector": {index_field_name: {
                                "topk": top_k,
                                "query": query_vectors[:nq],
                                "metric_type": real_metric_type,
                                "params": search_param}
                            }}
                            result = milvus_instance.query(vector_query)
                        elif name in ["flush", "compact"]:
                            func = getattr(milvus_instance, name)
                            func()
                        logger.debug(milvus_instance.count())
                    except Exception as e:
                        logger.error(name)
                        logger.error(str(e))
                        raise
                logger.debug("Loop time: %d" % i)
            # end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            end_row_count = milvus_instance.count()
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info,
                                         {})
            metric.metrics = {
                "type": "stability",
                "value": {
                    "during_time": during_time,
                    "row_count_increments": end_row_count - start_row_count
                }
            }
            report(metric)

        elif run_type == "debug":
            time.sleep(7200)
            default_insert_vectors = [[random.random() for _ in range(128)] for _ in range(500000)]
            interval = 50000
            for loop in range(1, 7):
                insert_xb = loop * interval
                insert_vectors = default_insert_vectors[:insert_xb]
                insert_ids = [i for i in range(insert_xb)]
                entities = milvus_instance.generate_entities(insert_vectors, insert_ids)
                for j in range(5):
                    milvus_instance.insert(entities, ids=insert_ids)
                    time.sleep(10)

        else:
            raise Exception("Run type not defined")
        logger.debug("All test finished")
