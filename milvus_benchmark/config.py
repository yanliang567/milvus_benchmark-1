# MONGO_SERVER = 'mongodb://192.168.1.234:27017/'
MONGO_SERVER = 'mongodb://mongodb.test:27017/'

SCHEDULER_DB = "scheduler"
JOB_COLLECTION = "jobs"

REGISTRY_URL = "registry.zilliz.com/milvus-distributed/milvus-distributed"
IDC_NAS_URL = "//172.16.70.249/test"

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530

HELM_NAMESPACE = "milvus"
BRANCH = "0331"

DEFAULT_CPUS = 32

RAW_DATA_DIR = "/test/milvus/raw_data/"

# nars log
LOG_PATH = "/test/milvus/benchmark/logs/{}/".format(BRANCH)
