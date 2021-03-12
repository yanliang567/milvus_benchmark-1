import logging
from .base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.locust")


class LocustRunner(BaseRunner):
    """run insert"""
    name = "LocustRunner"

    def __init__(self, env, metric):
        super(LocustRunner, self).__init__(env, metric)

    def run(self, run_params):
    	pass