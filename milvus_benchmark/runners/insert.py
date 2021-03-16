import time
import pdb
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.insert")


class InsertRunner(BaseRunner):
    """run insert"""
    name = "InsertRunner"

    def __init__(self, env, metric):
        super(InsertRunner, self).__init__(env, metric)
        self.run_as_group = False

    def extract_cases(self, suite):
        collection = suite["collection"]
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"]
        index_info = {}
        search_params = {}
        vector_type = utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = utils.get_default_field_name(vector_type)
        flush = True
        if "flush" in collection and collection["flush"] == "no":
            flush = False
        self.update_metric(self.name, collection_info, index_info, None)
        case_metric = copy.deepcopy(self.metric)
        case_metrics = list()
        case_params = list()
        if self.run_as_group is False:
            case_metrics.append(case_metric)
            case_param = {
                "collection_name": collection_name,
                "dimension": dimension,
                "collection_size": collection_size,
                "ni_per": ni_per,
                "metric_type": metric_type,
                "vector_type": vector_type,
                "other_fields": other_fields,
                "build_index": build_index,
                "flush_after_insert": flush,
                "index_field_name": index_field_name,
                "index_type": index_type,
                "index_param": index_param,
            }
            case_params.append(case_param)
        return case_params, case_metrics

    # TODO: error handler
    def run_case(self, case_metric, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        index_field_name = case_param["index_field_name"]

        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(case_param["dimension"], data_type=case_param["vector_type"],
                                          other_fields=case_param["other_fields"], collection_name=collection_name)
        if build_index is True:
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            logger.debug(self.milvus.describe_index(index_field_name))
        self.insert_from_files(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        flush_time = 0.0
        build_time = 0.0
        if case_param["flush_after_insert"] is True:
            start_time = time.time()
            self.milvus.flush()
            flush_time = time.time() - start_time
            logger.debug(self.milvus.count())
        if case_param["build_index"] is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            build_time = time.time() - start_time
        case_metric.update_result({"flush_time": flush_time, "build_time": build_time})
