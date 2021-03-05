from redis import Redis
from rq import Queue
import config


task_queue = Queue(connection=Redis(config.REDIS_URI))

