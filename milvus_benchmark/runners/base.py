import time
import pdb
import logging
import threading
import grpc
import numpy as np

from milvus_benchmark.env import get_env
from milvus_benchmark import config
from milvus_benchmark.client import MilvusClient
from . import utils

logger = logging.getLogger("milvus_benchmark.runners.base")


class BaseRunner(object):
    """runner is actually the executors"""

    def __init__(self, env, metric):
        self._metric = metric
        self._env = env
        self._result = dict()
        self._milvus = MilvusClient(host=self._env.hostname)

    def run(self, run_params):
        pass

    def stop(self):
        logger.debug("Start clean up env: {} in runner".format(self.env.name))

    @property
    def milvus(self):
        return self._milvus

    @property
    def metric(self):
        return self._metric

    @property
    def result(self):
        return self._result

    def report_metric(self, collection_info, index_info, search_params, run_params={}):
        self._metric.collection = collection_info
        self._metric.index = index_info
        self._metric.search = search_params
        self._metric.run_params = run_params
        return self._metric

    # TODO: need to improve
    def insert_from_files(self, milvus, collection_name, data_type, dimension, size, ni):
        total_time = 0.0
        qps = 0.0
        ni_time = 0.0
        vectors_per_file = utils.get_len_vectors_per_file(data_type, dimension)
        logger.debug(vectors_per_file)
        logger.debug(size)
        logger.debug(ni)
        if size % vectors_per_file or size % ni:
            logger.error("Not invalid collection size or ni")
            return False
        i = 0
        while i < (size // vectors_per_file):
            vectors = []
            if vectors_per_file >= ni:
                file_name = utils.gen_file_name(i, dimension, data_type)
                # logger.info("Load npy file: %s start" % file_name)
                data = np.load(file_name)
                # logger.info("Load npy file: %s end" % file_name)
                for j in range(vectors_per_file // ni):
                    vectors = data[j * ni:(j + 1) * ni].tolist()
                    if vectors:
                        # start insert vectors
                        start_id = i * vectors_per_file + j * ni
                        end_id = start_id + len(vectors)
                        logger.debug("Start id: %s, end id: %s" % (start_id, end_id))
                        ids = [k for k in range(start_id, end_id)]
                        entities = milvus.generate_entities(vectors, ids)
                        ni_start_time = time.time()
                        try:
                            res_ids = milvus.insert(entities, ids=ids)
                        except grpc.RpcError as e:
                            if e.code() == grpc.StatusCode.UNAVAILABLE:
                                logger.debug("Retry insert")

                                def retry():
                                    res_ids = milvus.insert(entities, ids=ids)

                                t0 = threading.Thread(target=retry)
                                t0.start()
                                t0.join()
                                logger.debug("Retry successfully")
                            raise e
                        # assert ids == res_ids
                        # milvus.flush()
                        ni_end_time = time.time()
                        logger.debug(milvus.count())
                        total_time = total_time + ni_end_time - ni_start_time
                i += 1
            else:
                vectors.clear()
                loops = ni // vectors_per_file
                for j in range(loops):
                    file_name = utils.gen_file_name(loops * i + j, dimension, data_type)
                    data = np.load(file_name)
                    vectors.extend(data.tolist())
                if vectors:
                    start_id = i * vectors_per_file
                    end_id = start_id + len(vectors)
                    logger.info("Start id: %s, end id: %s" % (start_id, end_id))
                    ids = [k for k in range(start_id, end_id)]
                    entities = milvus.generate_entities(vectors, ids)
                    ni_start_time = time.time()
                    try:
                        res_ids = milvus.insert(entities, ids=ids)
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            logger.debug("Retry insert")

                            def retry():
                                res_ids = milvus.insert(entities, ids=ids)

                            t0 = threading.Thread(target=retry)
                            t0.start()
                            t0.join()
                            logger.debug("Retry successfully")
                        raise e

                    # assert ids == res_ids
                    # milvus.flush()
                    ni_end_time = time.time()
                    logger.debug(milvus.count())
                    total_time = total_time + ni_end_time - ni_start_time
                i += loops
        rps = round(size / total_time, 2)
        ni_time = round(total_time / (size / ni), 2)
        self._result.update({
            "total_time": round(total_time, 2),
            "rps": rps,
            "ni_time": ni_time
        })
