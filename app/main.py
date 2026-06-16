from fastapi import FastAPI

from app.api import router
from app.config import settings

app = FastAPI(
    title=settings.service_name,
    version="0.1.1",
    description="pgAssistant Collector API",
)

app.include_router(router)
