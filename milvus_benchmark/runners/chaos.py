import time
import pdb
import copy
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.chaos")


def run_step(milvus, interface_name, interface_params):
    if interface_name == "create":
        collection_name = interface_params["name"]
        (data_type, _, dimension, _) = parser.collection_parser(collection_name)
        milvus.set_collection(collection_name)
        vector_type = utils.get_vector_type(data_type)
        milvus.create_collection(dimension, data_type=vector_type)
    else:
        func = getattr(milvus, interface_name)
        func(**interface_params)


class SimpleChaosRunner(BaseRunner):
    """run chaos"""
    name = "simple_chaos"

    def __init__(self, env, metric):
        super(SimpleChaosRunner, self).__init__(env, metric)

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
            run_step(self.milvus, interface_name, params)

    def run_case(self, case_metric, **case_param):
        processing = case_param["processing"]
        assertions = case_param["assertions"]
        logger.debug(processing)
        logger.debug(assertions)