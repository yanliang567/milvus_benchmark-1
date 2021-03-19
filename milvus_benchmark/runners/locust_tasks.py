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
        X = utils.generate_vectors(self.params["nq"], self.info["dimension"])
        vector_query = {"vector": {self.info["index_field_name"]: {
            "topk": self.params["top_k"], 
            "query": X, 
            "metric_type": "L2", 
            "params": self.params["search_param"]}
        }}
        filter_query = []
        for filter in self.params["filters"]:
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

    # @task
    # def get(self):
    #     self.client.get()

    # @task
    # def delete(self):
    #     self.client.delete([random.randint(1, 1000000)], log=False)

    def insert(self):
        ids = [random.randint(1, 10000000) for _ in range(self.params["ni_per"])]
        X = [[random.random() for _ in range(dim)] for _ in range(self.params["ni_per"])]
        entities = utils.generate_entities(self.info["collection_info"], X, ids)
        self.client.insert(entities, ids, log=False)

    @task
    def insert_rand(self):
        self.client.insert_rand(log=False)
