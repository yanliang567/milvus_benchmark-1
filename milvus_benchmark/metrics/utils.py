import logging
from pymongo import MongoClient

from .models.env import Env
from .models.hardware import Hardware
from .models.metric import Metric
from .models.server import Server
from .config import MONGO_SERVER, DB, UNIQUE_ID_COLLECTION, DOC_COLLECTION

_client = MongoClient(MONGO_SERVER)


def insert_or_get(md5):
    collection = _client[DB][UNIQUE_ID_COLLECTION]
    found = collection.find_one({'md5': md5})
    if not found:
        return collection.insert_one({'md5': md5}).inserted_id
    return found['_id']


def save(obj):
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
