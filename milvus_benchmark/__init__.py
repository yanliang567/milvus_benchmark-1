import logging
from typing import Optional
from fastapi import FastAPI
from milvus_benchmark.routers import tasks

app = FastAPI()
app.include_router(tasks.router)