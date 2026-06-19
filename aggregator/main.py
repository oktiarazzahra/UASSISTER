from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from redis.asyncio import Redis

from . import database
from .dedup import process_event
from .models import Event, EventBatch
from .worker import GROUP_NAME, consume_loop

logger = logging.getLogger(__name__)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI):
	"""Inisialisasi pool database dan koneksi Redis saat aplikasi start."""
	db_pool: asyncpg.Pool | None = None
	redis_client: Redis | None = None
	worker_tasks: list[asyncio.Task[None]] = []
	started_at = time.monotonic()

	try:
		db_pool = await database.init_db()
		redis_url = os.getenv("REDIS_URL", "redis://broker:6379/0")
		redis_client = Redis.from_url(redis_url, decode_responses=True)
		await redis_client.ping()
		app.state.db_pool = db_pool
		app.state.redis = redis_client
		app.state.started_at = started_at
		if os.getenv("ENABLE_WORKERS", "0") == "1":
			worker_tasks = [
				asyncio.create_task(consume_loop(f"worker-{index + 1}", redis_client, db_pool))
				for index in range(4)
			]
			app.state.worker_tasks = worker_tasks
			logger.info("Consumer worker 1-4 dimulai")
		logger.info("Application startup complete")
		yield
	finally:
		for task in worker_tasks:
			task.cancel()
		if worker_tasks:
			await asyncio.gather(*worker_tasks, return_exceptions=True)
		if redis_client is not None:
			await redis_client.close()
		if db_pool is not None:
			await db_pool.close()


app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)

app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
	"""Catat method, path, status code, dan latency setiap request."""
	start = time.perf_counter()
	response = await call_next(request)
	latency_ms = (time.perf_counter() - start) * 1000
	logger.info(
		"%s %s -> %s (%.2f ms)",
		request.method,
		request.url.path,
		response.status_code,
		latency_ms,
	)
	return response


def _get_db_pool(request: Request) -> asyncpg.Pool:
	"""Ambil pool database dari state aplikasi."""
	db_pool = getattr(request.app.state, "db_pool", None)
	if db_pool is None:
		raise HTTPException(status_code=503, detail={"status": "db not ready"})
	return db_pool


def _get_redis(request: Request) -> Redis:
	"""Ambil client Redis dari state aplikasi."""
	redis_client = getattr(request.app.state, "redis", None)
	if redis_client is None:
		raise HTTPException(status_code=503, detail={"status": "broker not ready"})
	return redis_client


async def _ensure_ready(request: Request) -> tuple[asyncpg.Pool, Redis]:
	"""Pastikan DB dan Redis siap dipakai sebelum request diproses."""
	db_pool = _get_db_pool(request)
	redis_client = _get_redis(request)
	try:
		async with db_pool.acquire() as connection:
			await connection.fetchval("SELECT 1")
		await redis_client.ping()
	except Exception as exc:
		raise HTTPException(status_code=503, detail={"status": "service unavailable"}) from exc
	return db_pool, redis_client


async def _ensure_db_ready(request: Request) -> asyncpg.Pool:
	"""Pastikan hanya koneksi database yang siap dipakai."""
	db_pool = _get_db_pool(request)
	try:
		async with db_pool.acquire() as connection:
			await connection.fetchval("SELECT 1")
	except Exception as exc:
		raise HTTPException(status_code=503, detail={"status": "db not ready"}) from exc
	return db_pool


def _normalize_events(payload: Any) -> list[Event]:
	"""Ubah payload tunggal, array raw, atau EventBatch menjadi daftar Event tervalidasi."""
	try:
		if isinstance(payload, list):
			batch = EventBatch(events=payload)
			return batch.events
		if isinstance(payload, dict) and "events" in payload:
			batch = EventBatch.model_validate(payload)
			return batch.events
		return [Event.model_validate(payload)]
	except ValidationError as exc:
		raise HTTPException(status_code=422, detail=exc.errors()) from exc


@app.post("/publish")

async def publish_event(request: Request, payload: Any = Body(...)):
	"""Terima event tunggal atau batch, lalu push ke Redis stream per topic."""
	db_pool, redis_client = await _ensure_ready(request)

	events = _normalize_events(payload)
	if not events:
		return JSONResponse({"received": 0, "message": "Tidak ada event untuk diproses"})

	process_sync = os.getenv("PROCESS_EVENTS_SYNC", "0") == "1"
	for event in events:
		message_id = await redis_client.xadd(
			f"events:{event.topic}",
			{"event": event.model_dump_json()},
		)
		if process_sync:
			await process_event(event, db_pool)
			try:
				await redis_client.xgroup_create(f"events:{event.topic}", GROUP_NAME, id="0", mkstream=True)
			except Exception as exc:
				if "BUSYGROUP" not in str(exc):
					raise
			await redis_client.xack(f"events:{event.topic}", GROUP_NAME, message_id)

	return JSONResponse(
		{"received": len(events), "message": f"Berhasil menerima {len(events)} event"}
	)


@app.get("/events")
async def get_events(
	request: Request,
	topic: str | None = None,
	limit: int = Query(default=100, ge=1, le=1000),
	offset: int = Query(default=0, ge=0),
):
	"""Ambil daftar event unik yang sudah diproses dari Postgres."""
	db_pool = await _ensure_db_ready(request)
	query = """
		SELECT topic, event_id, source, payload, received_at
		FROM processed_events
		WHERE ($1::text IS NULL OR topic = $1)
		ORDER BY received_at DESC
		LIMIT $2 OFFSET $3
	"""
	async with db_pool.acquire() as connection:
		rows = await connection.fetch(query, topic, limit, offset)

	events = [
		{
			"topic": row["topic"],
			"event_id": row["event_id"],
			"source": row["source"],
			"payload": row["payload"],
			"received_at": row["received_at"].isoformat(),
		}
		for row in rows
	]
	return JSONResponse(events)


@app.get("/stats")
async def get_stats(request: Request):
	"""Kembalikan statistik pemrosesan dan daftar topic yang pernah diterima."""
	db_pool = await _ensure_db_ready(request)
	async with db_pool.acquire() as connection:
		stats_row = await connection.fetchrow(
			"""
			SELECT received, unique_processed, duplicate_dropped, started_at
			FROM stats
			WHERE id = 1
			"""
		)
		topics = await connection.fetch(
			"SELECT DISTINCT topic FROM processed_events ORDER BY topic ASC"
		)

	if stats_row is None:
		raise HTTPException(status_code=503, detail={"status": "stats not ready"})

	uptime_seconds = int(time.monotonic() - getattr(request.app.state, "started_at", time.monotonic()))
	return JSONResponse(
		{
			"received": int(stats_row["received"]),
			"unique_processed": int(stats_row["unique_processed"]),
			"duplicate_dropped": int(stats_row["duplicate_dropped"]),
			"topics": [row["topic"] for row in topics],
			"uptime_seconds": uptime_seconds,
		}
	)


@app.get("/health")
async def health(request: Request):
	"""Cek kesehatan koneksi database dan Redis."""
	db_status = "disconnected"
	broker_status = "disconnected"

	try:
		db_pool = _get_db_pool(request)
		async with db_pool.acquire() as connection:
			await connection.fetchval("SELECT 1")
		db_status = "connected"
	except Exception:
		db_status = "disconnected"

	try:
		redis_client = _get_redis(request)
		await redis_client.ping()
		broker_status = "connected"
	except Exception:
		broker_status = "disconnected"

	return JSONResponse({"status": "ok", "db": db_status, "broker": broker_status})
