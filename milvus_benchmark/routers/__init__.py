from pydantic import BaseModel
from typing import Callable
from fastapi import Request, Body, Response
from fastapi.routing import APIRoute
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse, JSONResponse


class ResponseListModel(BaseModel):
    msg: str = ""
    code: int = 200
    data: list = []


class ResponseDictModel(BaseModel):
    msg: str = ""
    code: int = 200
    data: dict = []


class ValidationRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> JSONResponse:
            try:
                return await original_route_handler(request)
            except RequestValidationError as exc:
                code = 422
                return JSONResponse(status_code=code, content={"code": code, "msg": exc.errors()})
        return custom_route_handler