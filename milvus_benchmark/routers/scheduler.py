import logging
import traceback
from fastapi import APIRouter
from milvus_benchmark.scheduler import scheduler
from milvus_benchmark.routers import ResponseDictModel

logger = logging.getLogger("milvus_benchmark.routers.scheduler")


router = APIRouter(
    prefix="/scheduler",
    tags=["scheduler"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def get_scheduler_status():
    data={"status": scheduler.running}
    return ResponseDictModel(data=data)


@router.post("/restart")
def restart_scheduler():
    msg = "restart scheduler success"
    try:
        if scheduler.running:
            scheduler.resume()
        else:
            scheduler.start()
    except Exception as e:
        msg = str(e)
        code = 500
        logger.error(traceback.format_exc())
        return ResponseDictModel(code=code, msg=msg)
    return ResponseDictModel(msg=msg)