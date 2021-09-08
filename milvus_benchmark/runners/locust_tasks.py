import pdb
import random
import time
import logging
import math 
from locust import TaskSet, task, SequentialTaskSet
from . import utils

logger = logging.getLogger("milvus_benchmark.runners.locust_tasks")


# class Tasks(TaskSet):
class Tasks(SequentialTaskSet):
    @task
    def query(self):
        op = "query"
        # X = utils.generate_vectors(self.params[op]["nq"], self.op_info["dimension"])
        vector_query = {"vector": {self.op_info["vector_field_name"]: {
            "topk": self.params[op]["top_k"], 
            "query": self.values["X"][:self.params[op]["nq"]], 
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
        self.client.query(vector_query, filter_query=filter_query, log=False, timeout=30)

    @task
    def flush(self):
        op = 'flush'
        collection_name = self.params[op]['collection_name'] if 'collection_name' in self.params[op] else self.client._collection_name
        self.client.flush(collection_name=collection_name, log=False, timeout=30)

    @task
    def load(self):
        self.client.load_collection(timeout=30)

    @task
    def release(self):
        self.client.release_collection()
        self.client.load_collection(timeout=30)

    # @task
    # def release_index(self):
    #     self.client.release_index()

    # @task
    # def create_index(self):
    #     self.client.release_index()

    @task
    def insert(self):
        op = 'insert'
        collection_name = self.params[op]['collection_name'] if (self.params[op] is not None) and ('collection_name' in self.params[op]) else self.client._collection_name
        # ids = [random.randint(1000000, 10000000) for _ in range(self.params[op]["ni_per"])]
        # X = [[random.random() for _ in range(self.op_info["dimension"])] for _ in range(self.params[op]["ni_per"])]
        if collection_name is self.client._collection_name:
            entities = utils.generate_entities(self.op_info["collection_info"], self.values["X"][:self.params[op]["ni_per"]], self.values["ids"][:self.params[op]["ni_per"]])
        else:
            entities = utils.generate_entities(self.client.get_info(collection_name=collection_name),
                                               self.values["X"][:self.params[op]["ni_per"]],
                                               self.values["ids"][:self.params[op]["ni_per"]])
        self.client.insert(entities, collection_name=collection_name, log=False)

    @task
    def insert_flush(self):
        op = "insert_flush"
        # ids = [random.randint(1000000, 10000000) for _ in range(self.params[op]["ni_per"])]
        # X = [[random.random() for _ in range(self.op_info["dimension"])] for _ in range(self.params[op]["ni_per"])]
        entities = utils.generate_entities(self.op_info["collection_info"], self.values["X"][:self.params[op]["ni_per"]], self.values["ids"][:self.params[op]["ni_per"]])
        self.client.insert(entities, log=False)
        self.client.flush(log=False)
        
    @task
    def insert_rand(self):
        self.client.insert_rand(log=False)

    @task
    def get(self):
        op = "get"
        # ids = [random.randint(1, 10000000) for _ in range(self.params[op]["ids_length"])]
        self.client.get(self.values["get_ids"][:self.params[op]["ids_length"]])

    @task
    def create_collection(self):
        op = 'create_collection'
        if self.client.exists_collection():
            logger.debug("Start drop collection")
            self.client.drop()
            time.sleep(2)
        dim = self.params[op]['dim'] if 'dim' in self.params[op] else 128
        collection_name = self.params[op]['collection_name'] if (self.params[op] is not None) and ('collection_name' in self.params[op]) else self.client._collection_name
        self.client.create_collection(dimension=dim, collection_name=collection_name)

    @task
    def create_index(self):
        op = 'create_index'
        collection_name = self.params[op]['collection_name'] if (self.params[op] is not None) and ('collection_name' in self.params[op]) else self.client._collection_name
        index_type = self.params[op]['index_type'] if 'index_type' in self.params[op] else "ivf_sq8"
        index_param = self.params[op]['index_param'] if 'index_param' in self.params[op] else None
        self.client.create_index(field_name='float_vector', index_type=index_type, metric_type='l2', collection_name=collection_name, index_param=index_param)

    @task
    def drop_collection(self):
        op = 'drop_collection'
        collection_name = self.params[op]['collection_name'] if (self.params[op] is not None) and ('collection_name' in self.params[op]) else self.client._collection_name
        self.client.drop(collection_name=collection_name)
