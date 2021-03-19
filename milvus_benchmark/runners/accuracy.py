import json
import time
import copy
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.accuracy")


class AccuracyRunner(BaseRunner):
    """run accuracy"""
    name = "accuracy"

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