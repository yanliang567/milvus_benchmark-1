from redis import Redis
from rq import Queue
import config


queue = Queue(connection=Redis(config.REDIS_URI))

