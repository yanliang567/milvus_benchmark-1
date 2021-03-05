import sys
import pdb
import random
import logging
import json
import time, datetime
import traceback
from multiprocessing import Process
from milvus import Milvus, DataType
import numpy as np
import utils

logger = logging.getLogger("milvus_benchmark.client")

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
INDEX_MAP = {
    "flat": "FLAT",
    "ivf_flat": "IVF_FLAT",
    "ivf_sq8": "IVF_SQ8",
    "nsg": "NSG",
    "ivf_sq8h": "IVF_SQ8_HYBRID",
    "ivf_pq": "IVF_PQ",
    "hnsw": "HNSW",
    "annoy": "ANNOY",
    "bin_flat": "BIN_FLAT",
    "bin_ivf_flat": "BIN_IVF_FLAT",
    "rhnsw_pq": "RHNSW_PQ",
    "rhnsw_sq": "RHNSW_SQ"
}
epsilon = 0.1


def time_wrapper(func):
    """
    This decorator prints the execution time for the decorated function.
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        # logger.debug("Milvus {} start".format(func.__name__))
        log = kwargs.get("log", True)
        kwargs.pop("log", None)
        result = func(*args, **kwargs)
        end = time.time()
        if log:
            logger.debug("Milvus {} run in {}s".format(func.__name__, round(end - start, 2)))
        return result

    return wrapper


class MilvusClient(object):
    def __init__(self, collection_name=None, host=None, port=None, timeout=180):
        self._collection_name = collection_name
        start_time = time.time()
        if not host:
            host = SERVER_HOST_DEFAULT
        if not port:
            port = SERVER_PORT_DEFAULT
        logger.debug(host)
        logger.debug(port)
        # retry connect remote server
        i = 0
        while time.time() < start_time + timeout:
            try:
                self._milvus = Milvus(
                    host=host,
                    port=port,
                    try_connect=False,
                    pre_ping=False)
                break
            except Exception as e:
                logger.error(str(e))
                logger.error("Milvus connect failed: %d times" % i)
                i = i + 1
                time.sleep(i)

        if time.time() > start_time + timeout:
            raise Exception("Server connect timeout")
        # self._metric_type = None

    def __str__(self):
        return 'Milvus collection %s' % self._collection_name

    def check_status(self, status):
        if not status.OK():
            logger.error(status.message)
            logger.error(self._milvus.server_status())
            logger.error(self.count())
            raise Exception("Status not ok")

    def check_result_ids(self, result):
        for index, item in enumerate(result):
            if item[0].distance >= epsilon:
                logger.error(index)
                logger.error(item[0].distance)
                raise Exception("Distance wrong")

    # only support the given field name
    def create_collection(self, dimension, data_type=DataType.FLOAT_VECTOR, auto_id=False,
                          collection_name=None, other_fields=None):
        self._dimension = dimension
        if not collection_name:
            collection_name = self._collection_name
        vec_field_name = utils.get_default_field_name(data_type)
        fields = [{"name": vec_field_name, "type": data_type, "params": {"dim": dimension}}]
        if other_fields:
            other_fields = other_fields.split(",")
            if "int" in other_fields:
                fields.append({"name": utils.DEFAULT_INT_FIELD_NAME, "type": DataType.INT64})
            if "float" in other_fields:
                fields.append({"name": utils.DEFAULT_FLOAT_FIELD_NAME, "type": DataType.FLOAT})
        create_param = {
            "fields": fields,
            "auto_id": auto_id}
        try:
            self._milvus.create_collection(collection_name, create_param)
            logger.info("Create collection: <%s> successfully" % collection_name)
        except Exception as e:
            logger.error(str(e))
            raise

    def create_partition(self, tag, collection_name=None):
        if not collection_name:
            collection_name = self._collection_name
        self._milvus.create_partition(collection_name, tag)

    def generate_values(self, data_type, vectors, ids):
        values = None
        if data_type in [DataType.INT32, DataType.INT64]:
            values = ids
        elif data_type in [DataType.FLOAT, DataType.DOUBLE]:
            values = [(i + 0.0) for i in ids]
        elif data_type in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
            values = vectors
        return values

    def generate_entities(self, vectors, ids=None, collection_name=None):
        entities = []
        if collection_name is None:
            collection_name = self._collection_name
        info = self.get_info(collection_name)
        for field in info["fields"]:
            field_type = field["type"]
            entities.append(
                {"name": field["name"], "type": field_type, "values": self.generate_values(field_type, vectors, ids)})
        return entities

    @time_wrapper
    def insert(self, entities, ids=None, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        try:
            insert_ids = self._milvus.insert(tmp_collection_name, entities, ids=ids)
            return insert_ids
        except Exception as e:
            logger.error(str(e))

    def get_dimension(self):
        info = self.get_info()
        for field in info["fields"]:
            if field["type"] in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
                return field["params"]["dim"]

    def get_rand_ids(self, length):
        segment_ids = []
        while True:
            stats = self.get_stats()
            segments = stats["partitions"][0]["segments"]
            # random choice one segment
            segment = random.choice(segments)
            try:
                segment_ids = self._milvus.list_id_in_segment(self._collection_name, segment["id"])
            except Exception as e:
                logger.error(str(e))
            if not len(segment_ids):
                continue
            elif len(segment_ids) > length:
                return random.sample(segment_ids, length)
            else:
                logger.debug("Reset length: %d" % len(segment_ids))
                return segment_ids

    # def get_rand_ids_each_segment(self, length):
    #     res = []
    #     status, stats = self._milvus.get_collection_stats(self._collection_name)
    #     self.check_status(status)
    #     segments = stats["partitions"][0]["segments"]
    #     segments_num = len(segments)
    #     # random choice from each segment
    #     for segment in segments:
    #         status, segment_ids = self._milvus.list_id_in_segment(self._collection_name, segment["name"])
    #         self.check_status(status)
    #         res.extend(segment_ids[:length])
    #     return segments_num, res

    # def get_rand_entities(self, length):
    #     ids = self.get_rand_ids(length)
    #     status, get_res = self._milvus.get_entity_by_id(self._collection_name, ids)
    #     self.check_status(status)
    #     return ids, get_res

    def get(self):
        get_ids = random.randint(1, 1000000)
        self._milvus.get_entity_by_id(self._collection_name, [get_ids])

    @time_wrapper
    def get_entities(self, get_ids):
        get_res = self._milvus.get_entity_by_id(self._collection_name, get_ids)
        return get_res

    @time_wrapper
    def delete(self, ids, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        self._milvus.delete_entity_by_id(tmp_collection_name, ids)

    def delete_rand(self):
        delete_id_length = random.randint(1, 100)
        count_before = self.count()
        logger.debug("%s: length to delete: %d" % (self._collection_name, delete_id_length))
        delete_ids = self.get_rand_ids(delete_id_length)
        self.delete(delete_ids)
        self.flush()
        logger.info("%s: count after delete: %d" % (self._collection_name, self.count()))
        get_res = self._milvus.get_entity_by_id(self._collection_name, delete_ids)
        for item in get_res:
            assert not item
        # if count_before - len(delete_ids) < self.count():
        #     logger.error(delete_ids)
        #     raise Exception("Error occured")

    @time_wrapper
    def flush(self,_async=False, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        self._milvus.flush([tmp_collection_name], _async=_async)

    @time_wrapper
    def compact(self, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        status = self._milvus.compact(tmp_collection_name)
        self.check_status(status)

    @time_wrapper
    def create_index(self, field_name, index_type, metric_type, _async=False, index_param=None):
        index_type = INDEX_MAP[index_type]
        metric_type = utils.metric_type_trans(metric_type)
        logger.info("Building index start, collection_name: %s, index_type: %s, metric_type: %s" % (
            self._collection_name, index_type, metric_type))
        if index_param:
            logger.info(index_param)
        index_params = {
            "index_type": index_type,
            "metric_type": metric_type,
            "params": index_param
        }
        self._milvus.create_index(self._collection_name, field_name, index_params, _async=_async)

    # TODO: need to check
    def describe_index(self, field_name):
        # stats = self.get_stats()
        info = self._milvus.describe_index(self._collection_name, field_name)
        index_info = {"index_type": "flat", "index_param": None}
        for field in info["fields"]:
            for index in field['indexes']:
                if not index or "index_type" not in index:
                    continue
                else:
                    for k, v in INDEX_MAP.items():
                        if index['index_type'] == v:
                            index_info['index_type'] = k
                            index_info['index_param'] = index['params']
                            return index_info
        return index_info

    def drop_index(self, field_name):
        logger.info("Drop index: %s" % self._collection_name)
        return self._milvus.drop_index(self._collection_name, field_name)

    @time_wrapper
    def query(self, vector_query, filter_query=None, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        must_params = [vector_query]
        if filter_query:
            must_params.extend(filter_query)
        query = {
            "bool": {"must": must_params}
        }
        result = self._milvus.search(tmp_collection_name, query)
        return result

    @time_wrapper
    def load_and_query(self, vector_query, filter_query=None, collection_name=None):
        tmp_collection_name = self._collection_name if collection_name is None else collection_name
        must_params = [vector_query]
        if filter_query:
            must_params.extend(filter_query)
        query = {
            "bool": {"must": must_params}
        }
        self.load_collection(tmp_collection_name)
        result = self._milvus.search(tmp_collection_name, query)
        return result

    def get_ids(self, result):
        idss = result._entities.ids
        ids = []
        len_idss = len(idss)
        len_r = len(result)
        top_k = len_idss // len_r
        for offset in range(0, len_idss, top_k):
            ids.append(idss[offset: min(offset + top_k, len_idss)])
        return ids

    def query_rand(self, nq_max=100):
        # for ivf search
        dimension = 128
        top_k = random.randint(1, 100)
        nq = random.randint(1, nq_max)
        nprobe = random.randint(1, 100)
        search_param = {"nprobe": nprobe}
        query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
        metric_type = random.choice(["l2", "ip"])
        logger.info("%s, Search nq: %d, top_k: %d, nprobe: %d" % (self._collection_name, nq, top_k, nprobe))
        vec_field_name = utils.get_default_field_name()
        vector_query = {"vector": {vec_field_name: {
            "topk": top_k,
            "query": query_vectors,
            "metric_type": utils.metric_type_trans(metric_type),
            "params": search_param}
        }}
        self.query(vector_query)

    def load_query_rand(self, nq_max=100):
        # for ivf search
        dimension = 128
        top_k = random.randint(1, 100)
        nq = random.randint(1, nq_max)
        nprobe = random.randint(1, 100)
        search_param = {"nprobe": nprobe}
        query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
        metric_type = random.choice(["l2", "ip"])
        logger.info("%s, Search nq: %d, top_k: %d, nprobe: %d" % (self._collection_name, nq, top_k, nprobe))
        vec_field_name = utils.get_default_field_name()
        vector_query = {"vector": {vec_field_name: {
            "topk": top_k,
            "query": query_vectors,
            "metric_type": utils.metric_type_trans(metric_type),
            "params": search_param}
        }}
        self.load_and_query(vector_query)

    # TODO: need to check
    def count(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        row_count = self._milvus.get_collection_stats(collection_name)["row_count"]
        logger.debug("Row count: %d in collection: <%s>" % (row_count, collection_name))
        return row_count

    def drop(self, timeout=120, collection_name=None):
        timeout = int(timeout)
        if collection_name is None:
            collection_name = self._collection_name
        logger.info("Start delete collection: %s" % collection_name)
        self._milvus.drop_collection(collection_name)
        i = 0
        while i < timeout:
            try:
                row_count = self.count(collection_name=collection_name)
                if row_count:
                    time.sleep(1)
                    i = i + 1
                    continue
                else:
                    break
            except Exception as e:
                logger.debug(str(e))
                break
        if i >= timeout:
            logger.error("Delete collection timeout")

    def get_stats(self):
        return self._milvus.get_collection_stats(self._collection_name)

    def get_info(self, collection_name=None):
        # pdb.set_trace()
        if collection_name is None:
            collection_name = self._collection_name
        return self._milvus.get_collection_info(collection_name)

    def show_collections(self):
        return self._milvus.list_collections()

    def exists_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        res = self._milvus.has_collection(collection_name)
        return res

    def clean_db(self):
        collection_names = self.show_collections()
        for name in collection_names:
            self.drop(collection_name=name)

    @time_wrapper
    def load_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        return self._milvus.load_collection(collection_name, timeout=3000)

    @time_wrapper
    def release_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        return self._milvus.release_collection(collection_name, timeout=3000)

    @time_wrapper
    def load_partitions(self, tag_names, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        return self._milvus.load_partitions(collection_name, tag_names, timeout=3000)

    @time_wrapper
    def release_partitions(self, tag_names, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        return self._milvus.release_partitions(collection_name, tag_names, timeout=3000)

    # TODO: remove
    # def get_server_version(self):
    #     return self._milvus.server_version()

    # def get_server_mode(self):
    #     return self.cmd("mode")

    # def get_server_commit(self):
    #     return self.cmd("build_commit_id")

    # def get_server_config(self):
    #     return json.loads(self.cmd("get_milvus_config"))

    # def get_mem_info(self):
    #     result = json.loads(self.cmd("get_system_info"))
    #     result_human = {
    #         # unit: Gb
    #         "memory_used": round(int(result["memory_used"]) / (1024 * 1024 * 1024), 2)
    #     }
    #     return result_human

    # def cmd(self, command):
    #     res = self._milvus._cmd(command)
    #     logger.info("Server command: %s, result: %s" % (command, res))
    #     return res

    # @time_wrapper
    # def set_config(self, parent_key, child_key, value):
    #     self._milvus.set_config(parent_key, child_key, value)

    # def get_config(self, key):
    #     return self._milvus.get_config(key)
