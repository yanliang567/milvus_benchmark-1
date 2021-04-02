import os

MONGO_SERVER = 'mongodb://192.168.1.234:27017/'
# MONGO_SERVER = 'mongodb://mongodb.test:27017/'

SCHEDULER_DB = "scheduler"
JOB_COLLECTION = "jobs"

REGISTRY_URL = "registry.zilliz.com/milvus-distributed/milvus-distributed"
IDC_NAS_URL = "//172.16.70.249/test"

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530

HELM_NAMESPACE = "milvus"
BRANCH = "0331"
SERVER_VERSION = "2.0"
DEFAULT_CPUS = 48

RAW_DATA_DIR = "/test/milvus/raw_data/"

# nars log
LOG_PATH = "/test/milvus/benchmark/logs/{}/".format(BRANCH)

HELM_PATH = os.path.join(os.getcwd(), "../milvus-helm-charts/charts/milvus-ha")
DEFAULT_DEPLOY_MODE = "single"

NAMESPACE = "milvus"
DEFAULT_API_VERSION = 'chaos-mesh.org/v1alpha1'
DEFAULT_GROUP = 'chaos-mesh.org'
DEFAULT_VERSION = 'v1alpha1'

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 1
REDIS_LOG_EXPIRE_TIME = 100000