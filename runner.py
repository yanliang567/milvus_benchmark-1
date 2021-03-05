import os
import logging
import pdb
import time
import random
from multiprocessing import Process
from itertools import product
import numpy as np
import sklearn.preprocessing
from client import MilvusClient
import utils
import parser

logger = logging.getLogger("milvus_benchmark.runner")

VECTORS_PER_FILE = 1000000
SIFT_VECTORS_PER_FILE = 100000
JACCARD_VECTORS_PER_FILE = 2000000

MAX_NQ = 10001
FILE_PREFIX = "binary_"

# FOLDER_NAME = 'ann_1000m/source_data'
SRC_BINARY_DATA_DIR = '/test/milvus/raw_data/random/'
SIFT_SRC_DATA_DIR = '/test/milvus/raw_data/sift1b/'
DEEP_SRC_DATA_DIR = '/test/milvus/raw_data/deep1b/'
JACCARD_SRC_DATA_DIR = '/test/milvus/raw_data/jaccard/'
HAMMING_SRC_DATA_DIR = '/test/milvus/raw_data/jaccard/'
STRUCTURE_SRC_DATA_DIR = '/test/milvus/raw_data/jaccard/'
SIFT_SRC_GROUNDTRUTH_DATA_DIR = SIFT_SRC_DATA_DIR + 'gnd'

WARM_TOP_K = 1
WARM_NQ = 1
DEFAULT_DIM = 512


GROUNDTRUTH_MAP = {
    "1000000": "idx_1M.ivecs",
    "2000000": "idx_2M.ivecs",
    "5000000": "idx_5M.ivecs",
    "10000000": "idx_10M.ivecs",
    "20000000": "idx_20M.ivecs",
    "50000000": "idx_50M.ivecs",
    "100000000": "idx_100M.ivecs",
    "200000000": "idx_200M.ivecs",
    "500000000": "idx_500M.ivecs",
    "1000000000": "idx_1000M.ivecs",
}


def gen_file_name(idx, dimension, data_type):
    s = "%05d" % idx
    fname = FILE_PREFIX + str(dimension) + "d_" + s + ".npy"
    if data_type == "random":
        fname = SRC_BINARY_DATA_DIR+fname
    elif data_type == "sift":
        fname = SIFT_SRC_DATA_DIR+fname
    elif data_type == "deep":
        fname = DEEP_SRC_DATA_DIR+fname
    elif data_type == "jaccard":
        fname = JACCARD_SRC_DATA_DIR+fname
    elif data_type == "hamming":
        fname = HAMMING_SRC_DATA_DIR+fname
    elif data_type == "sub" or data_type == "super":
        fname = STRUCTURE_SRC_DATA_DIR+fname
    return fname


def get_vectors_from_binary(nq, dimension, data_type):
    # use the first file, nq should be less than VECTORS_PER_FILE
    if nq > MAX_NQ:
        raise Exception("Over size nq")
    if data_type == "random":
        file_name = SRC_BINARY_DATA_DIR+'query_%d.npy' % dimension
    elif data_type == "sift":
        file_name = SIFT_SRC_DATA_DIR+'query.npy'
    elif data_type == "deep":
        file_name = DEEP_SRC_DATA_DIR+'query.npy'
    elif data_type == "jaccard":
        file_name = JACCARD_SRC_DATA_DIR+'query.npy'
    elif data_type == "hamming":
        file_name = HAMMING_SRC_DATA_DIR+'query.npy'
    elif data_type == "sub" or data_type == "super":
        file_name = STRUCTURE_SRC_DATA_DIR+'query.npy'
    data = np.load(file_name)
    vectors = data[0:nq].tolist()
    return vectors


class Runner(object):
    def __init__(self):
        pass

    def normalize(self, metric_type, X):
        if metric_type == "ip":
            logger.info("Set normalize for metric_type: %s" % metric_type)
            X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
            X = X.astype(np.float32)
        elif metric_type == "l2":
            X = X.astype(np.float32)
        elif metric_type in ["jaccard", "hamming", "sub", "super"]:
            tmp = []
            for index, item in enumerate(X):
                new_vector = bytes(np.packbits(item, axis=-1).tolist())
                tmp.append(new_vector)
            X = tmp
        return X

    def generate_combinations(self, args):
        if isinstance(args, list):
            args = [el if isinstance(el, list) else [el] for el in args]
            return [list(x) for x in product(*args)]
        elif isinstance(args, dict):
            flat = []
            for k, v in args.items():
                if isinstance(v, list):
                    flat.append([(k, el) for el in v])
                else:
                    flat.append([(k, v)])
            return [dict(x) for x in product(*flat)]
        else:
            raise TypeError("No args handling exists for %s" % type(args).__name__)

    def do_insert(self, milvus, collection_name, data_type, dimension, size, ni):
        '''
        @params:
            mivlus: server connect instance
            dimension: collection dimensionn
            # index_file_size: size trigger file merge
            size: row count of vectors to be insert
            ni: row count of vectors to be insert each time
            # store_id: if store the ids returned by call add_vectors or not
        @return:
            total_time: total time for all insert operation
            qps: vectors added per second
            ni_time: avarage insert operation time
        '''
        bi_res = {}
        total_time = 0.0
        qps = 0.0
        ni_time = 0.0
        if data_type == "random":
            if dimension == 512:
                vectors_per_file = VECTORS_PER_FILE
            elif dimension == 4096:
                vectors_per_file = 100000
            elif dimension == 16384:
                vectors_per_file = 10000
        elif data_type == "sift":
            vectors_per_file = SIFT_VECTORS_PER_FILE
        elif data_type in ["jaccard", "hamming", "sub", "super"]:
            vectors_per_file = JACCARD_VECTORS_PER_FILE
        else:
            raise Exception("data_type: %s not supported" % data_type)
        if size % vectors_per_file or ni > vectors_per_file:
            raise Exception("Not invalid collection size or ni")
        file_num = size // vectors_per_file
        for i in range(file_num):
            file_name = gen_file_name(i, dimension, data_type)
            # logger.info("Load npy file: %s start" % file_name)
            data = np.load(file_name)
            # logger.info("Load npy file: %s end" % file_name)
            loops = vectors_per_file // ni
            for j in range(loops):
                vectors = data[j*ni:(j+1)*ni].tolist()
                if vectors:
                    ni_start_time = time.time()
                    # start insert vectors
                    start_id = i * vectors_per_file + j * ni
                    end_id = start_id + len(vectors)
                    logger.info("Start id: %s, end id: %s" % (start_id, end_id))
                    ids = [k for k in range(start_id, end_id)]
                    status, ids = milvus.insert(vectors, ids=ids)
                    # milvus.flush()
                    logger.debug(milvus.count())
                    ni_end_time = time.time()
                    total_time = total_time + ni_end_time - ni_start_time

        qps = round(size / total_time, 2)
        ni_time = round(total_time / (loops * file_num), 2)
        bi_res["total_time"] = round(total_time, 2)
        bi_res["qps"] = qps
        bi_res["ni_time"] = ni_time
        return bi_res

    def do_query(self, milvus, collection_name, top_ks, nqs, run_count=1, search_param=None):
        bi_res = []
        (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)
        for nq in nqs:
            tmp_res = []
            vectors = base_query_vectors[0:nq]
            for top_k in top_ks:
                avg_query_time = 0.0
                min_query_time = 0.0
                logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
                for i in range(run_count):
                    logger.info("Start run query, run %d of %s" % (i+1, run_count))
                    start_time = time.time()
                    query_res = milvus.query(vectors, top_k, search_param=search_param)
                    interval_time = time.time() - start_time
                    if (i == 0) or (min_query_time > interval_time):
                        min_query_time = interval_time
                logger.info("Min query time: %.2f" % min_query_time)
                tmp_res.append(round(min_query_time, 2))
            bi_res.append(tmp_res)
        return bi_res

    def do_query_qps(self, milvus, query_vectors, top_k, search_param):
        start_time = time.time()
        result = milvus.query(query_vectors, top_k, search_param) 
        end_time = time.time()
        return end_time - start_time

    def do_query_ids(self, milvus, collection_name, top_k, nq, search_param=None):
        (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)
        vectors = base_query_vectors[0:nq]
        logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
        query_res = milvus.query(vectors, top_k, search_param=search_param)
        result_ids = []
        result_distances = []
        for result in query_res:
            tmp = []
            tmp_distance = []
            for item in result:
                tmp.append(item.id)
                tmp_distance.append(item.distance)
            result_ids.append(tmp)
            result_distances.append(tmp_distance)
        return result_ids, result_distances

    def do_query_acc(self, milvus, collection_name, top_k, nq, id_store_name, search_param=None):
        (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)
        vectors = base_query_vectors[0:nq]
        logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
        query_res = milvus.query(vectors, top_k, search_param=None)
        # if file existed, cover it
        if os.path.isfile(id_store_name):
            os.remove(id_store_name)
        with open(id_store_name, 'a+') as fd:
            for nq_item in query_res:
                for item in nq_item:
                    fd.write(str(item.id)+'\t')
                fd.write('\n')

    # compute and print accuracy
    def compute_accuracy(self, flat_file_name, index_file_name):
        flat_id_list = []; index_id_list = []
        logger.info("Loading flat id file: %s" % flat_file_name)
        with open(flat_file_name, 'r') as flat_id_fd:
            for line in flat_id_fd:
                tmp_list = line.strip("\n").strip().split("\t")
                flat_id_list.append(tmp_list)
        logger.info("Loading index id file: %s" % index_file_name)
        with open(index_file_name) as index_id_fd:
            for line in index_id_fd:
                tmp_list = line.strip("\n").strip().split("\t")
                index_id_list.append(tmp_list)
        if len(flat_id_list) != len(index_id_list):
            raise Exception("Flat index result length: <flat: %s, index: %s> not match, Acc compute exiting ..." % (len(flat_id_list), len(index_id_list)))
        # get the accuracy
        return self.get_recall_value(flat_id_list, index_id_list)

    def get_recall_value(self, true_ids, result_ids):
        """
        Use the intersection length
        """
        sum_radio = 0.0
        for index, item in enumerate(result_ids):
            # tmp = set(item).intersection(set(flat_id_list[index]))
            tmp = set(true_ids[index]).intersection(set(item))
            sum_radio = sum_radio + len(tmp) / len(item)
            # logger.debug(sum_radio)
        return round(sum_radio / len(result_ids), 3)

    """
    Implementation based on:
        https://github.com/facebookresearch/faiss/blob/master/benchs/datasets.py
    """
    def get_groundtruth_ids(self, collection_size):
        fname = GROUNDTRUTH_MAP[str(collection_size)]
        fname = SIFT_SRC_GROUNDTRUTH_DATA_DIR + "/" + fname
        a = np.fromfile(fname, dtype='int32')
        d = a[0]
        true_ids = a.reshape(-1, d + 1)[:, 1:].copy()
        return true_ids
