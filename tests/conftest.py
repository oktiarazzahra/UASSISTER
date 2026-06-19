from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4, UUID

import asyncpg
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PROCESS_EVENTS_SYNC", "1")

from aggregator import database
from aggregator.main import app
from aggregator.models import Event

def _running_in_docker() -> bool:
	"""Deteksi apakah proses pytest berjalan di dalam container Docker.

	Mengecek file /.dockerenv lebih dapat diandalkan dibanding mencoba
	resolve DNS, karena di Windows, nama host pendek seperti 'storage'
	kadang tetap lolos getaddrinfo() lewat NetBIOS/LLMNR meski sebenarnya
	bukan service Compose, sehingga baru gagal belakangan saat koneksi
	asli dibuka.
	"""
	return Path("/.dockerenv").exists() or os.getenv("RUNNING_IN_DOCKER") == "1"


def _resolve_default_host(service_name: str) -> str:
	"""Gunakan nama service Compose di dalam container, fallback ke localhost di luar."""
	return service_name if _running_in_docker() else "127.0.0.1"

DEFAULT_POSTGRES_DSN = os.getenv(
	"TEST_POSTGRES_DSN",
	os.getenv(
		"POSTGRES_DSN",
		f"postgresql://pubsub:pubsub123@{_resolve_default_host('storage')}:5432/pubsub",
	),
)
DEFAULT_REDIS_URL = os.getenv(
	"TEST_REDIS_URL",
	os.getenv(
		"REDIS_URL",
		f"redis://{_resolve_default_host('broker')}:6379/15",
	),
)


@pytest_asyncio.fixture
async def event_factory() -> Callable[..., Event]:
	"""Buat factory untuk menghasilkan Event valid dengan nilai yang bisa dioverride."""
	def _factory(
		*,
		topic: str = "user.login",
		event_id: str | None = None,
		timestamp: str | None = None,
		source: str = "publisher-test",
		payload: dict | None = None,
	) -> Event:
		return Event(
			topic=topic,
			event_id=event_id or str(uuid4()),
			timestamp=timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
			source=source,
			payload=payload or {"value": 1, "message": "tes"},
		)

	return _factory


@pytest_asyncio.fixture
async def async_db_pool() -> asyncpg.Pool:
	"""Sediakan pool koneksi Postgres untuk pengujian integrasi."""
	pool = await database.init_db(DEFAULT_POSTGRES_DSN)
	try:
		yield pool
	finally:
		await pool.close()


@pytest_asyncio.fixture
async def redis_client() -> Redis:
	"""Sediakan koneksi Redis untuk pengujian integrasi."""
	client = Redis.from_url(DEFAULT_REDIS_URL, decode_responses=True)
	await client.ping()
	try:
		yield client
	finally:
		await client.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_storage(async_db_pool: asyncpg.Pool, redis_client: Redis):
	"""Kosongkan data antar test agar hasil pengujian tetap deterministik."""
	await _reset_database(async_db_pool)
	await redis_client.flushdb()
	yield
	await _reset_database(async_db_pool)
	await redis_client.flushdb()


@pytest_asyncio.fixture
async def test_client(async_db_pool: asyncpg.Pool, redis_client: Redis) -> AsyncClient:
	"""Sediakan HTTP client async ke FastAPI app tanpa menjalankan server nyata."""
	app.state.db_pool = async_db_pool
	app.state.redis = redis_client
	app.state.started_at = time.monotonic()
	transport = ASGITransport(app=app)
	async with AsyncClient(transport=transport, base_url="http://test") as client:
		yield client


async def _reset_database(pool: asyncpg.Pool) -> None:
	"""Reset tabel inti agar setiap test dimulai dari keadaan bersih."""
	async with pool.acquire() as connection:
		async with connection.transaction():
			await connection.execute("TRUNCATE TABLE processed_events")
			await connection.execute(
				"""
				UPDATE stats
				SET received = 0,
					unique_processed = 0,
					duplicate_dropped = 0,
					started_at = NOW()
				WHERE id = 1
				"""
			)