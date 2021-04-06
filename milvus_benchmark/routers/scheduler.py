import logging
import traceback
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
    msg = "restart scheduler success"
    try:
        if scheduler.running:
            scheduler.shutdown()
        scheduler.start()
    except Exception as e:
        msg = str(e)
        logger.error(traceback.format_exc())
    return msg