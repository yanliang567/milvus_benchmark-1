import pdb
import random
import time
import logging
from locust import TaskSet, task
from . import utils

dim = 128

logger = logging.getLogger("milvus_benchmark.runners.locust_tasks")


class Tasks(TaskSet):
    @task
    def query(self):
        op = "query"
        X = utils.generate_vectors(self.params[op]["nq"], self.op_info["dimension"])
        vector_query = {"vector": {self.op_info["index_field_name"]: {
            "topk": self.params[op]["top_k"], 
            "query": X, 
            "metric_type": "L2", 
            "params": self.params[op]["search_param"]}
        }}
        filter_query = []
        if "filters" in self.params[op]:
            for filter in self.params[op]["filters"]:
                filter_param = []
                if isinstance(filter, dict) and "range" in filter:
                    filter_query.append(eval(filter["range"]))
                    # filter_param.append(filter["range"])
                if isinstance(filter, dict) and "term" in filter:
                    filter_query.append(eval(filter["term"]))
                    # filter_param.append(filter["term"])
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

    # @task
    # def release_index(self):
    #     self.client.release_index()

    # @task
    # def create_index(self):
    #     self.client.release_index()

    def insert(self):
        op = "insert"
        ids = [random.randint(1, 10000000) for _ in range(self.params[op]["ni_per"])]
        X = [[random.random() for _ in range(dim)] for _ in range(self.params[op]["ni_per"])]
        entities = utils.generate_entities(self.op_info["collection_info"], X, ids)
        self.client.insert(entities, ids, log=False)

    @task
    def insert_rand(self):
        self.client.insert_rand(log=False)
