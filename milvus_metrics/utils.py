from milvus_metrics.models.env import Env
from milvus_metrics.models.hardware import Hardware
from milvus_metrics.models.metric import Metric
from milvus_metrics.models.server import Server
from milvus_metrics.config import MONGO_SERVER, DB, UNIQUE_ID_COLLECTION, DOC_COLLECTION, HOST_COLLECTION
from pymongo import MongoClient
import logging

_client = MongoClient(MONGO_SERVER)


def insert_or_get(md5):
    collection = _client[DB][UNIQUE_ID_COLLECTION]
    found = collection.find_one({'md5': md5})
    if not found:
        return collection.insert_one({'md5': md5}).inserted_id
    return found['_id']


def report(obj):
    if not isinstance(obj, Metric):
        logging.error("obj is not instance of Metric")
        return False

    if not isinstance(obj.server, Server):
        logging.error("obj.server is not instance of Server")
        return False

    if not isinstance(obj.hardware, Hardware):
        logging.error("obj.hardware is not instance of Hardware")
        return False

    if not isinstance(obj.env, Env):
        logging.error("obj.env is not instance of Env")
        return False

    md5 = obj.server.json_md5()
    server_doc_id = insert_or_get(md5)
    obj.server = {"id": server_doc_id, "value": vars(obj.server)}

    md5 = obj.hardware.json_md5()
    hardware_doc_id = insert_or_get(md5)
    obj.hardware = {"id": hardware_doc_id, "value": vars(obj.hardware)}

    md5 = obj.env.json_md5()
    env_doc_id = insert_or_get(md5)
    obj.env = {"id": env_doc_id, "value": vars(obj.env)}

    collection = _client[DB][DOC_COLLECTION]
    collection.insert_one(vars(obj))


def get_cpus_by_host(host):
    collection = _client[DB][HOST_COLLECTION]
    host_doc = collection.find_one({"name": host})
    if host_doc:
        return int(host_doc["cpus"])
    else:
        return False
