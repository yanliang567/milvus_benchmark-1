import logging
import uuid
import json
import traceback
from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from milvus_benchmark.scheduler import scheduler
from milvus_benchmark.task import run_suite
from milvus_benchmark.db.model import Task as TaskModel
from milvus_benchmark.db.model import TaskStatus
from milvus_benchmark.routers import ResponseListModel, ResponseDictModel, ValidationRoute

logger = logging.getLogger("milvus_benchmark.routers.tasks")


class Task(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    env_mode: str
    env_params: Optional[dict] = None
    config: Optional[str] = None
    suite: dict
    status: Optional[str] = TaskStatus.NEW.value


router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    responses={404: {"msg": "Not found"}},
)
router.route_class = ValidationRoute


@router.get("/")
def get_tasks():
    # tasks = scheduler.get_jobs()
    try:
        tasks = TaskModel.objects.values().raw({"name": {"$exists": True, "$ne": None}})
        return ResponseListModel(data=list(tasks))
    except Exception as e:
        msg = "get tasks failed: {}".format(str(e))
        logger.error(traceback.format_exc())
        return ResponseListModel(code=500, msg=msg)


@router.get("/{task_id}")
def get_task(task_id: str):
    # job = scheduler.get_job(task_id)
    try:
        task = TaskModel.objects.values().get({"_id": task_id})
        return ResponseDictModel(data=task)
    except Exception as e:
        msg = "get tasks by id <{}> failed: {}".format(task_id, str(e))
        logger.error(traceback.format_exc())
        return ResponseDictModel(code=500, msg=msg)


@router.post("/add", response_model=ResponseDictModel)
def add_task(task: Task):
    try:
        suite = task.suite
        env_mode = task.env_mode
        env_params = task.env_params
        name = task.name
        description = task.description
        job_id = str(uuid.uuid4())
        job = scheduler.add_job(run_suite, args=[job_id, suite, env_mode, env_params], misfire_grace_time=30, id=job_id)
        now_time = datetime.now()
        TaskModel(job_id, name, description, env_mode=env_mode, env_params=env_params, suite=suite, created_time=now_time, last_executed_time=now_time).save()
        return ResponseDictModel(data={"job": str(job), "id": job_id})
    except Exception as e:
        logger.error(traceback.format_exc())
        msg = "add task failed: {}".format(str(e))
        return ResponseDictModel(code=500, msg=msg)


@router.post("/update/{task_id}")
def update_task(task_id: str, task: Task):
    try:
        suite = task.suite
        env_mode = task.env_mode
        env_params = task.env_params
        name = task.name
        description = task.description
        TaskModel(task_id, name, description, env_mode=env_mode, env_params=env_params, suite=suite).save()
        return ResponseDictModel()
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        msg = "update task failed: {}".format(str(e))
        return ResponseDictModel(code=500, msg=msg)    


@router.delete("/delete/{task_id}")
def delete_task(task_id: str):
    try:
        task = Task.objects.get({"task_id": task_id})
        task.delete()
    except Exception as e:
        logger.error(str(e))
        msg = "Delete task {} failed with error {}".format(task_id, str(e))
        return ResponseDictModel(code=500, msg=msg)
    return ResponseDictModel(code=200, msg="Delete successful")


@router.post("/reschedule/{task_id}")
def reschedule_task(task_id: str):
    try:
        task = TaskModel.objects.get({"_id": task_id})
    except Exception as e:
        msg = "Task: {} not found, {}".format(task_id, str(e))
        logger.error(msg)
        return ResponseDictModel(code=500, msg=msg)
    try:
        # task_son = task.to_son()
        # logger.debug(task_son["suite"])
        job = scheduler.add_job(run_suite, args=[task_id, task.suite, task.env_mode, task.env_params], misfire_grace_time=30, id=task_id)
        logger.debug(str(job))
        task.update_time()
        task.save()
        return ResponseDictModel(data={"job": str(job)})
    except Exception as e:
        logger.error(str(e))
        msg = "Task: {} reschedule failed".format(task_id)
        logger.error(traceback.format_exc())
        return ResponseDictModel(code=500, msg=msg)
