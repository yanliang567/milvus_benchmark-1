import logging
from fastapi import APIRouter
from pydantic import BaseModel
from milvus_benchmark.scheduler import scheduler


logger = logging.getLogger("milvus_benchmark.routers.tasks")


class Task(BaseModel):
    name: str
    description: Optional[str] = None
    env_mode: Optional[str] = None
    env_params: Optional[str] = None
    config: Optional[str] = None
    suite: str


router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def get_tasks():
    return {"Hello": "World"}


@router.post("/add")
def add_task(task: Task):
	logger.debug(task)
    return {"Hello": "World"}


@router.put("/{task_id}")
def update_task(task_id: int, q: Optional[str] = None):
    return {"task_id": task_id, "q": q}


 @router.delete("/delete/{task_id}")
def delete_task(task_id: int):
    return {"task_id": task_id}