import pdb
import random
import time
import logging
import math 
from locust import TaskSet, task
from . import utils

logger = logging.getLogger("milvus_benchmark.runners.locust_tasks")


class Tasks(TaskSet):
    def setup(self):
        nq = 10000
        nb = 100000
        self.ids = [random.randint(1000000, 10000000) for _ in range(nb)]
        self.get_ids = [random.randint(1, 10000000) for _ in range(nb)]
        self.X = utils.generate_vectors(nq, self.op_info["dimension"])

    @task
    def query(self):
        op = "query"
        # X = utils.generate_vectors(self.params[op]["nq"], self.op_info["dimension"])
        vector_query = {"vector": {self.op_info["vector_field_name"]: {
            "topk": self.params[op]["top_k"], 
            "query": self.X[:self.params[op]["nq"]], 
            "metric_type": self.params[op]["metric_type"] if "metric_type" in self.params[op] else utils.DEFAULT_METRIC_TYPE, 
            "params": self.params[op]["search_param"]}
        }}
        filter_query = []
        if "filters" in self.params[op]:
            for filter in self.params[op]["filters"]:
                if isinstance(filter, dict) and "range" in filter:
                    filter_query.append(eval(filter["range"]))
                if isinstance(filter, dict) and "term" in filter:
                    filter_query.append(eval(filter["term"]))
        # logger.debug(filter_query)
        self.client.query(vector_query, filter_query=filter_query, log=False)

    @task
    def flush(self):
        self.client.flush(log=False)

    @task
    def load(self):
        self.client.load_collection()

    @task
    def release(self):
        self.client.release_collection()
        self.client.load_collection()

    # @task
    # def release_index(self):
    #     self.client.release_index()

    # @task
    # def create_index(self):
    #     self.client.release_index()

    @task
    def insert(self):
        op = "insert"
        # ids = [random.randint(1000000, 10000000) for _ in range(self.params[op]["ni_per"])]
        # X = [[random.random() for _ in range(self.op_info["dimension"])] for _ in range(self.params[op]["ni_per"])]
        entities = utils.generate_entities(self.op_info["collection_info"], self.X[:self.params[op]["ni_per"]], self.ids[:self.params[op]["ni_per"]])
        self.client.insert(entities, log=False)

    @task
    def insert_flush(self):
        op = "insert_flush"
        # ids = [random.randint(1000000, 10000000) for _ in range(self.params[op]["ni_per"])]
        # X = [[random.random() for _ in range(self.op_info["dimension"])] for _ in range(self.params[op]["ni_per"])]
        entities = utils.generate_entities(self.op_info["collection_info"], self.X[:self.params[op]["ni_per"]], self.ids[:self.params[op]["ni_per"]])
        self.client.insert(entities, log=False)
        self.client.flush(log=False)
        
    @task
    def insert_rand(self):
        self.client.insert_rand(log=False)

    @task
    def get(self):
        op = "get"
        # ids = [random.randint(1, 10000000) for _ in range(self.params[op]["ids_length"])]
        self.client.get(self.get_ids[:self.params[op]["ids_length"]])
