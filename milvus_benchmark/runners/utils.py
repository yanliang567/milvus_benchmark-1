import os
import pdb
import logging
import numpy as np
import sklearn.preprocessing
import h5py

from milvus import DataType
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.runners.utils")

DELETE_INTERVAL_TIME = 10

VECTORS_PER_FILE = 1000000
SIFT_VECTORS_PER_FILE = 100000
BINARY_VECTORS_PER_FILE = 2000000

MAX_NQ = 10001
FILE_PREFIX = "binary_"

WARM_TOP_K = 1
WARM_NQ = 1
DEFAULT_DIM = 512

RANDOM_SRC_DATA_DIR = config.RAW_DATA_DIR+'random/'
SIFT_SRC_DATA_DIR = config.RAW_DATA_DIR+'sift1b/'
DEEP_SRC_DATA_DIR = config.RAW_DATA_DIR+'deep1b/'
BINARY_SRC_DATA_DIR = config.RAW_DATA_DIR+'binary/'
SIFT_SRC_GROUNDTRUTH_DATA_DIR = SIFT_SRC_DATA_DIR + 'gnd'

DEFAULT_F_FIELD_NAME = 'float_vector'
DEFAULT_B_FIELD_NAME = 'binary_vector'
DEFAULT_INT_FIELD_NAME = 'int64'
DEFAULT_FLOAT_FIELD_NAME = 'float'

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

METRIC_MAP = {
    "l2": "L2",
    "ip": "IP",
    "jaccard": "JACCARD",
    "hamming": "HAMMING",
    "sub": "SUBSTRUCTURE",
    "super": "SUPERSTRUCTURE"
}


def get_len_vectors_per_file(data_type, dimension):
    if data_type == "random":
        if dimension == 512:
            vectors_per_file = VECTORS_PER_FILE
        elif dimension == 4096:
            vectors_per_file = 100000
        elif dimension == 16384:
            vectors_per_file = 10000
    elif data_type == "sift":
        vectors_per_file = SIFT_VECTORS_PER_FILE
    elif data_type in ["binary"]:
        vectors_per_file = BINARY_VECTORS_PER_FILE
    else:
        raise Exception("data_type: %s not supported" % data_type)
    return vectors_per_file


def metric_type_trans(metric_type):
    if metric_type in METRIC_MAP.keys():
        return METRIC_MAP[metric_type]
    else:
        raise Exception("metric_type: %s not in METRIC_MAP" % metric_type)


def get_dataset(hdf5_file_path):
    if not os.path.exists(hdf5_file_path):
        raise Exception("%s not existed" % hdf5_file_path)
    dataset = h5py.File(hdf5_file_path)
    return dataset


def get_default_field_name(data_type=DataType.FLOAT_VECTOR):
    if data_type == DataType.FLOAT_VECTOR:
        field_name = DEFAULT_F_FIELD_NAME
    elif data_type == DataType.BINARY_VECTOR:
        field_name = DEFAULT_B_FIELD_NAME
    elif data_type == DataType.INT64:
        field_name = DEFAULT_INT_FIELD_NAME
    elif data_type == DataType.FLOAT:
        field_name = DEFAULT_FLOAT_FIELD_NAME
    else:
        logger.error(data_type)
        raise Exception("Not supported data type")
    return field_name

        
def get_vector_type(data_type):
    vector_type = ''
    if data_type in ["random", "sift", "deep", "glove"]:
        vector_type = DataType.FLOAT_VECTOR
    elif data_type in ["binary"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        raise Exception("Data type: %s not defined" % data_type)
    return vector_type


def get_vector_type_from_metric(metric_type):
    vector_type = ''
    if metric_type in ["hamming", "jaccard"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        vector_type = DataType.FLOAT_VECTOR
    return vector_type


def normalize(metric_type, X):
    if metric_type == "ip":
        logger.info("Set normalize for metric_type: %s" % metric_type)
        X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(np.float32)
    elif metric_type == "l2":
        X = X.astype(np.float32)
    elif metric_type in ["jaccard", "hamming", "sub", "super"]:
        tmp = []
        for item in X:
            new_vector = bytes(np.packbits(item, axis=-1).tolist())
            tmp.append(new_vector)
        X = tmp
    return X


def generate_combinations(args):
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
