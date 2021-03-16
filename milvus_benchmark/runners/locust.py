import logging
from .base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.locust")


class LocustRunner(BaseRunner):
    """run insert"""
    name = "locust_performance"

    def __init__(self, env, metric):
        super(LocustRunner, self).__init__(env, metric)

    def run_case(self, case_metric, **case_param):
    	pass