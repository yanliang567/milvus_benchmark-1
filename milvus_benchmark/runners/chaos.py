import time
import pdb
import copy
import logging
from milvus_benchmark import parser
from milvus_benchmark import utils
from milvus_benchmark.runners import utils as runner_utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.chaos")


class SimpleChaosRunner(BaseRunner):
    """run chaos"""
    name = "simple_chaos"

    def __init__(self, env, metric):
        super(SimpleChaosRunner, self).__init__(env, metric)

    def run_step(self, interface_name, interface_params):
        if interface_name == "create_collection":
            collection_name = utils.get_unique_name("chaos")
            self.data_type = interface_params["data_type"]
            self.dimension = interface_params["dimension"]
            self.milvus.set_collection(collection_name)
            vector_type = runner_utils.get_vector_type(self.data_type)
            self.milvus.create_collection(self.dimension, data_type=vector_type)
        elif interface_name == "insert":
            batch_size = interface_params["batch_size"]
            collection_size = interface_params["collection_size"]
            self.insert_local(self.milvus, self.milvus.collection_name, self.data_type, self.dimension, collection_size, batch_size)
        elif interface_name == "create_index":
            metric_type = interface_params["metric_type"]
            index_type = interface_params["index_type"]
            index_param = interface_params["index_param"]
            field_name = runner_utils.get_default_field_name(vector_type)
            self.milvus.create_index(field_name, index_type, metric_type, index_param=index_param)
        elif interface_name == "flush":
            self.milvus.flush()

    def extract_cases(self, collection):
        before_steps = collection["before"]
        after = collection["after"] if "after" in collection else None
        processing = collection["processing"]
        assertions = collection["assertions"]
        case_metrics = []
        case_params = [{
            "before_steps": before_steps,
            "after": after,
            "processing": processing,
            "assertions": assertions
        }]
        self.init_metric(self.name, {}, {}, None)
        case_metric = copy.deepcopy(self.metric)
        case_metrics.append(case_metric)
        return case_params, case_metrics

    def prepare(self, **case_param):
        steps = case_param["before_steps"]
        for step in steps:
            interface_name = step["interface_name"]
            params = step["params"]
            self.run_step(interface_name, params)

    def run_case(self, case_metric, **case_param):
        processing = case_param["processing"]
        assertions = case_param["assertions"]
        pass