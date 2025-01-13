from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from gateway_service.models import ErrorResponse
from gateway_service.routers import api, manage

app = FastAPI(title="Library System Gateway", version="1.0")

app.include_router(api.router)
app.include_router(manage.router)


@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(message=exc.detail).model_dump(mode="json"),
    )
