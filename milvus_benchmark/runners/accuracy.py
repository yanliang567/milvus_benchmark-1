import json
import time
import copy
import logging
import numpy as np

from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.accuracy")
INSERT_INTERVAL = 2000


class AccuracyRunner(BaseRunner):
    """run accuracy"""
    name = "accuracy"

    def __init__(self, env, metric):
        super(AccuracyRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        base_query_vectors = utils.get_vectors_from_binary(utils.MAX_NQ, dimension, data_type)
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name
        }
        index_info = self.milvus.describe_index(index_field_name, collection_name)
        filters = collection["filters"] if "filters" in collection else []
        filter_query = []
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        search_params = collection["search_params"]
        search_params = utils.generate_combinations(search_params)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, index_info, search_info=None)
        for search_param in search_params:
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
                for nq in nqs:
                    query_vectors = base_query_vectors[0:nq]
                    for top_k in top_ks:
                        search_info = {
                            "topk": top_k,
                            "query": query_vectors,
                            "metric_type": utils.metric_type_trans(metric_type),
                            "params": search_param}
                        # TODO: only update search_info
                        case_metric = copy.deepcopy(self.metric)
                        case_metric.search = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "filter": filter_param
                        }
                        vector_query = {"vector": {index_field_name: search_info}}
                        case = {
                            "collection_name": collection_name,
                            "index_field_name": index_field_name,
                            "dimension": dimension,
                            "data_type": data_type,
                            "metric_type": metric_type,
                            "vector_type": vector_type,
                            "collection_size": collection_size,
                            "filter_query": filter_query,
                            "vector_query": vector_query
                        }
                        cases.append(case)
                        case_metrics.append(case_metric)
        return cases, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        self.milvus.set_collection(collection_name)
        if not self.milvus.exists_collection():
            logger.info("collection not exist")
        self.milvus.load_collection()

    def run_case(self, case_metric, **case_param):
        collection_size = case_param["collection_size"]
        nq = case_metric.search["nq"]
        top_k = case_metric.search["topk"]
        query_res = self.milvus.query(case_param["vector_query"], filter_query=case_param["filter_query"])
        true_ids_all = utils.get_ground_truth_ids(collection_size)
        logger.debug({"true_ids": [len(true_ids_all[0]), len(true_ids_all[0])]})
        result_ids = self.milvus.get_ids(query_res)
        logger.debug({"result_ids": len(result_ids[0])})
        acc_value = utils.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
        tmp_result = {"acc": acc_value}
        return tmp_result


class AccAccuracyRunner(BaseRunner):
    """run ann accuracy"""
    """
    1. entities from hdf5
    2. one collection test different index
    """
    name = "acc_accuracy"

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        base_query_vectors = utils.get_vectors_from_binary(utils.MAX_NQ, dimension, data_type)
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name
        }
        index_types = collection["index_types"]
        index_type = collection["index_type"]
        index_param = collection["index_param"]
        index_info = {
            "index_type": index_type,
            "index_param": index_param
        }
        filters = collection["filters"] if "filters" in collection else []
        filter_query = []
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        search_params = collection["search_params"]
        search_params = utils.generate_combinations(search_params)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, index_info, search_info=None)
        for search_param in search_params:
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
                for nq in nqs:
                    query_vectors = base_query_vectors[0:nq]
                    for top_k in top_ks:
                        search_info = {
                            "topk": top_k,
                            "query": query_vectors,
                            "metric_type": utils.metric_type_trans(metric_type),
                            "params": search_param}
                        # TODO: only update search_info
                        case_metric = copy.deepcopy(self.metric)
                        case_metric.search = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "filter": filter_param
                        }
                        vector_query = {"vector": {index_field_name: search_info}}
                        case = {
                            "collection_name": collection_name,
                            "index_field_name": index_field_name,
                            "dimension": dimension,
                            "data_type": data_type,
                            "vector_type": vector_type,
                            "collection_size": collection_size,
                            "filter_query": filter_query,
                            "vector_query": vector_query
                        }
                        cases.append(case)
                        case_metrics.append(case_metric)
        return cases, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        metric_type = case_param["metric_type"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        other_fields = case_param["other_fields"]
        info = self.milvus.get_info(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(dimension, data_type=vector_type, other_fields=other_fields)
        self.milvus.set_collection(collection_name)
        dataset = utils.get_dataset(hdf5_source_file)
        insert_vectors = utils.normalize(metric_type, np.array(dataset["train"]))
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
                    entities = utils.generate_entities(info, tmp_vectors.tolist(), ids)
                    res_ids = self.milvus.insert(entities, ids=ids)
                else:
                    entities = utils.generate_entities(info, tmp_vectors, ids)
                    res_ids = self.milvus.insert(entities, ids=ids)
                assert res_ids == ids
        self.milvus.flush()
        res_count = self.milvus.count()
        logger.info("Table: %s, row count: %d" % (collection_name, res_count))
        if res_count != len(insert_vectors):
            raise Exception("Table row count is not equal to insert vectors")


