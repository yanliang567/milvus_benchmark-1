import logging
import uuid
from fastapi import APIRouter, HTTPException, WebSocket
from typing import Optional
from pydantic import BaseModel
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from milvus_benchmark import redis_conn
from milvus_benchmark.scheduler import scheduler
from milvus_benchmark.task import run_suite

logger = logging.getLogger("milvus_benchmark.routers.tasks")


class Task(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    env_mode: Optional[str] = None
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
    tasks = scheduler.get_jobs()
    return str(tasks)


@router.get("/{task_id}")
def get_task(task_id: str):
    job = scheduler.get_job(task_id)
    return str(job)


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
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=400, detail="Suite key/value error")
    job_id = str(uuid.uuid4())
    job = scheduler.add_job(run_suite, args=[job_id, suite, env_mode, env_params], misfire_grace_time=30, id=job_id)
    return str(job)


@router.put("/{task_id}")
def update_task(task_id: str, q: Optional[str] = None):
    return {"task_id": task_id, "q": q}


@router.delete("/delete/{task_id}")
def delete_task(task_id: str):
    return {"task_id": task_id}