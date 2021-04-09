import sys
import logging
import traceback
import uvicorn
from typing import Optional
from fastapi import FastAPI
from milvus_benchmark.scheduler import scheduler
from milvus_benchmark.routers import tasks
from milvus_benchmark.routers import scheduler as scheduler_router
from milvus_benchmark.logs import log

log.setup_logging()
logger = logging.getLogger("milvus_benchmark.main")

app = FastAPI()
app.include_router(tasks.router)
app.include_router(scheduler_router.router)


# @app.on_event("startup")
# def init():
#     # init scheduler
#     if not scheduler.running:
#         scheduler.start()


# @app.on_event("shutdown")
# def tern_down():
#     # shutdown scheduler
#     if scheduler.running:
#         scheduler.shutdown(wait=False)


@app.get("/")
async def root():
    return {"message": "Hello Milvus!"}


if __name__ == "__main__":
    try:
        logger.debug(scheduler.running)
        if not scheduler.running:
            scheduler.start()
        uvicorn.run("main:app", port=8000, host='0.0.0.0', reload=True)
    except (KeyboardInterrupt, SystemExit):
        logger.error("Received interruption")
        scheduler.shutdown(wait=False)
        sys.exit(0)
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)
