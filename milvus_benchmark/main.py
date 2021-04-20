from gevent import monkey
monkey.patch_all(select=False)
import logging
import sys
import traceback

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware

from milvus_benchmark.logs import log
from milvus_benchmark.routers import scheduler as scheduler_router
from milvus_benchmark.routers import tasks
from milvus_benchmark.routers import websocket
from milvus_benchmark.scheduler import scheduler

log.setup_logging()
logger = logging.getLogger("milvus_benchmark.main")

origins = [
    "*"
    ]
middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"]
    )
]

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # expose_headers=["Access-Control-Allow-Origin"]
)
app.include_router(tasks.router)
app.include_router(scheduler_router.router)
app.include_router(websocket.router)
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
