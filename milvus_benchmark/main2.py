import app
from scheduler import scheduler


scheduler.start()

@app.get("/")
async def root():
    return {"message": "Hello Milvus!"}