import random
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient

connection_type = "single"
host = "192.168.1.29"
port = 19531
collection_name = "sift_128_euclidean"
dim = 128
m = MilvusClient(host=host, port=port, collection_name=collection_name)


class QueryTask(User):
    wait_time = between(0.001, 0.002)
    print("in query task")
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)

    @task()
    def query(self):
        top_k = 10
        X = [[random.random() for i in range(dim)] for i in range(1)]
        search_param = {"nprobe": 16}
        self.client.query(X, top_k, search_param)
