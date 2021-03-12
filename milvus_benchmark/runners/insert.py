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

    def run(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        self.milvus.set_collection(collection_name)
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"]
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(utils.DELETE_INTERVAL_TIME)
        index_info = {}
        search_params = {}
        vector_type = utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        self.milvus.create_collection(dimension, data_type=vector_type,
                                          other_fields=other_fields, collection_name=collection_name)
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = utils.get_default_field_name(vector_type)
            self.milvus.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            logger.debug(self.milvus.describe_index(index_field_name))
        self.insert_from_files(self.milvus, collection_name, data_type, dimension, collection_size, ni_per)
        flush_time = 0.0
        if "flush" in collection and collection["flush"] == "no":
            logger.debug("No manual flush")
        else:
            start_time = time.time()
            self.milvus.flush()
            flush_time = time.time() - start_time
            logger.debug(self.milvus.count())
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        total_time = res["total_time"]
        build_time = 0
        if build_index is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            build_time = time.time() - start_time
            total_time = total_time + build_time
        self.metric.metrics = {
            "type": self.name,
            "value": {
                "total_time": total_time,
                "qps": res["qps"],
                "ni_time": res["ni_time"],
                "flush_time": flush_time,
                "build_time": build_time
            }
        }
        logger.debug(self.metric.metrics)
