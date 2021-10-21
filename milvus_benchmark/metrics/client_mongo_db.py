import logging
import traceback

from pymongo import MongoClient

from .client_base import ClientBase

logger = logging.getLogger("milvus_benchmark.metric.client_mongo_db")


def mongodb_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                logger.error("[MongoDB Exception] %s" % str(traceback.format_exc()))
                return False
        return inner_wrapper
    return wrapper


class ClientMongoDB(ClientBase):

    def __init__(self, host, port=27017, dbname="test", collection_name="doc"):
        super().__init__()
        self.host = host
        self.port = port

        self.client = MongoClient(self.host, self.port)
        self.collection = self.get_collection(dbname, collection_name)

    @mongodb_try_catch()
    def query(self, query_data, collection=None):
        collection = self.collection if collection is None else collection
        logger.debug("[MongoDB API] Query data %s from mongoDB." % str(query_data))
        counts = collection.count_documents(query_data)
        if int(counts) > 0:
            logger.debug("[MongoDB API] %s query data already exist" % str(counts))
        res = collection.find_one(query_data)
        return res

    @mongodb_try_catch()
    def insert(self, insert_data, collection=None):
        collection = self.collection if collection is None else collection
        logger.debug("[MongoDB API] Insert data %s to mongoDB." % str(insert_data))
        res = collection.insert_one(insert_data)
        return res.inserted_id

    @mongodb_try_catch()
    def delete_one(self, delete_data, collection=None):
        collection = self.collection if collection is None else collection
        logger.debug("[MongoDB API] Delete data %s from mongoDB." % str(delete_data))
        res = collection.delete_one(delete_data)
        return res

    @mongodb_try_catch()
    def delete_all(self, delete_data, collection=None):
        collection = self.collection if collection is None else collection
        logger.debug("[MongoDB API] Delete all data %s from mongoDB." % str(delete_data))
        counts = collection.count_documents(delete_data)
        if int(counts) > 0:
            logger.debug("[MongoDB API] Prepare to delete %s pieces of data %s." % (str(counts), str(delete_data)))
        for count in range(1, int(counts+1)):
            collection.delete_one(delete_data)
            logger.debug("[MongoDB API] Delete %s data %s." % (str(count), str(delete_data)))

    @mongodb_try_catch()
    def get_collection(self, dbname, collection_name):
        db = self.create_database(dbname)
        collection = self.create_collection(db, collection_name)
        return collection

    @mongodb_try_catch()
    def create_database(self, dbname):
        databases = self.client.list_database_names()
        if dbname not in databases:
            logger.debug("[MongoDB API] Database %s does not exist." % str(dbname))
        else:
            logger.debug("[MongoDB API] Database %s already exists." % str(dbname))
        return self.client[dbname]

    @mongodb_try_catch()
    def create_collection(self, db, collection_name):
        collections = db.list_collection_names()
        if collection_name not in collections:
            logger.debug("[MongoDB API] Collection %s does not exist." % str(collection_name))
        else:
            logger.debug("[MongoDB API] Collection %s already exists." % str(collection_name))
        return db[collection_name]


if __name__ == "__main__":
    pass
    # MONGO_SERVER = ''
    # c = ClientMongoDB(MONGO_SERVER)
