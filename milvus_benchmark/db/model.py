from datetime import datetime
from enum import Enum
from pymongo import TEXT
from pymodm import connect, fields, MongoModel
# from pymodm.queryset import QuerySet
from milvus_benchmark import config

connect(config.MONGO_SERVER + config.TASK_DB)


class TaskStatus(Enum):
    NEW = 0
    RUNNING = 1
    EXECUTED = 2


class Task(MongoModel):
    task_id = fields.CharField(primary_key=True)
    name = fields.CharField(blank=True)
    description = fields.CharField(blank=True)
    env_mode = fields.CharField()
    env_params = fields.DictField()
    suite = fields.DictField()
    created_time = fields.DateTimeField()
    last_executed_time = fields.DateTimeField(blank=True)
    status = fields.CharField(blank=True)

    def update_time(self):
        self.last_executed_time = datetime.now()

    def update_status(self, status):
        self.status = status
