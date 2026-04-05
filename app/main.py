import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import create_tables
from app.worker.worker_runner import run_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting pms-sync-worker...")
    create_tables()
    worker_task = asyncio.create_task(run_worker())
    yield
    logger.info("Shutting down pms-sync-worker...")
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="PMS Sync Worker", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "pms-sync-worker"}
