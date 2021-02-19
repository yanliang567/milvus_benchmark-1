import time
import random
from multiprocessing import Process, Pool
from milvus import Milvus

HOST = "172.16.70.3"

collection_name = "sift_1m_1024_128_l2"
top_k = 1
nq = 10
dimension = 128
X = [[random.random() for _ in range(dimension)] for _ in range(nq)]
search_param = {"nprobe": 32}

thread_num = 2
start_time = time.time()


def query(port):
    print(port)
    m = Milvus(host=HOST, port=port, _try=False, _pre_ping=False)
    status, result = m.search(collection_name, top_k, query_records=X, params=search_param)
    assert status.OK()
    print(time.time() - start_time)


ports = []
for i in range(2):
    ports.append(19530 + i)
pool = Pool(processes=thread_num)
for p in ports:
    pool.apply_async(func=query, args=(p,))
pool.close()
pool.join()
