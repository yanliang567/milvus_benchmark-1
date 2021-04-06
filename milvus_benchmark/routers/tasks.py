import logging
import uuid
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from milvus_benchmark import redis_conn
from milvus_benchmark.scheduler import scheduler
from milvus_benchmark.task import run_suite
from milvus_benchmark.db.model import Task as TaskModel

logger = logging.getLogger("milvus_benchmark.routers.tasks")


class Task(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    env_mode: str
    env_params: Optional[dict] = None
    config: Optional[str] = None
    suite: dict


router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def get_tasks():
    # tasks = scheduler.get_jobs()
    tasks = TaskModel.objects.values().raw({"name": {"$exists": True, "$ne": None}})
    return list(tasks)


@router.get("/{task_id}")
def get_task(task_id: str):
    # job = scheduler.get_job(task_id)
    task = TaskModel.objects.get({"task_id": task_id})
    return task


@router.websocket_route("/ws/{task_id}")
async def task_ws_endpoint(websocket: WebSocket, task_id: str):
    while True:
        data = redis_conn.lrange(task_id, 0, 128)
        await websocket.send_text(f"{data}")


@router.post("/add")
def add_task(task: Task):
    try:
        suite = task.suite
        env_mode = task.env_mode
        env_params = task.env_params
        name = task.name
        description = task.description
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=400, detail="Suite key/value error")
    job_id = str(uuid.uuid4())
    TaskModel(job_id, name, description, env_mode=env_mode, env_params=env_params, suite=suite).save()
    job = scheduler.add_job(run_suite, args=[job_id, suite, env_mode, env_params], misfire_grace_time=30, id=job_id)
    return str(job)


@router.post("/{task_id}")
def update_task(task_id: str, task: Task):
    try:
        suite = task.suite
        env_mode = task.env_mode
        env_params = task.env_params
        name = task.name
        description = task.description
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=400, detail="Suite key/value error")
    TaskModel(task_id, name, description, env_mode=env_mode, env_params=env_params, suite=suite).save()


@router.delete("/delete/{task_id}")
def delete_task(task_id: str):
    return {"task_id": task_id}


@router.post("/{task_id}")
def schedule_task(task_id: str):
    task = TaskModel.objects.get({"task_id": task_id})
    if task:
        job = scheduler.add_job(run_suite, args=[task_id, task.suite, task.env_mode, task.env_params], misfire_grace_time=30, id=task_id)
        return str(job)
    else:
        raise HTTPException(status_code=400, detail="Task not found")