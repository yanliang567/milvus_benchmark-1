import logging
import traceback
from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from milvus_benchmark import redis_conn
from milvus_benchmark.routers import ResponseDictModel

logger = logging.getLogger("milvus_benchmark.routers.websocket")


router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


@router.websocket("/ws/{task_id}")
# async def task_ws_endpoint(self, websocket: WebSocket):
async def task_ws_endpoint(websocket: WebSocket, task_id: str):
    await manager.connect(websocket)
    try:
        while True:
            recv_data = await websocket.receive_text()
            logger.debug(recv_data)
            data = redis_conn.lrange(task_id, 0, -1)
            for item in data:
                await websocket.send_text(f"{item}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client left.")


html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://172.16.50.20:8000/ws/42ebb990-f84a-43e5-b229-c99483404aec");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


@router.get("/ws")
async def get():
    return HTMLResponse(html)