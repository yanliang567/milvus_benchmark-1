import logging
from fastapi import APIRouter
from milvus_benchmark.scheduler import scheduler

logger = logging.getLogger("milvus_benchmark.routers.scheduler")


router = APIRouter(
    prefix="/scheduler",
    tags=["scheduler"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def get_scheduler_status():
    return scheduler.running


@router.post("/restart")
def restart_scheduler():
    if scheduler.running:
        scheduler.shutdown()
    scheduler.start()