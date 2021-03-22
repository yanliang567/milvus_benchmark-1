import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor
import config
from pymongo import MongoClient

logger = logging.getLogger("milvus_benchmark.scheduler")

mongo_client = MongoClient(config.MONGO_SERVER)
jobstores = {
    'default': MongoDBJobStore(database=config.SCHEDULER_DB, collection=config.JOB_COLLECTION, client=mongo_client)
}

executors = {
    'default': ProcessPoolExecutor(20)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 32
}
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)