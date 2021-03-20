import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor
from . import config

logger = logging.getLogger("milvus_benchmark.scheduler")


jobstores = {
    'default': MongoDBJobStore(config.MONGO_SERVER),
}
executors = {
    'default': ProcessPoolExecutor(20)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 32
}
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)