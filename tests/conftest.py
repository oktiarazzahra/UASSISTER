from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from aggregator.database import init_db
from aggregator.main import app
from aggregator.models import Event


@pytest.fixture
def event_factory():
	"""Buat factory Event valid untuk dipakai di test."""
	def _factory(**overrides):
		base = {
			"topic": "user.login",
			"event_id": str(uuid.uuid4()),
			"timestamp": "2026-06-17T12:00:00+00:00",
			"source": "publisher-testhost",
			"payload": {"user_id": 1, "amount": 10.5},
		}
		base.update(overrides)
		return Event(**base)

	return _factory


@pytest_asyncio.fixture
async def async_db_pool():
	"""Buat koneksi pool Postgres untuk pengujian asinkron."""
	dsn = os.getenv("TEST_POSTGRES_DSN", os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@localhost:5432/pubsub"))
	pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
	async with pool.acquire() as connection:
		await connection.execute("TRUNCATE TABLE processed_events RESTART IDENTITY CASCADE")
		await connection.execute("UPDATE stats SET received = 0, unique_processed = 0, duplicate_dropped = 0, started_at = NOW() WHERE id = 1")
	try:
		yield pool
	finally:
		await pool.close()


@pytest_asyncio.fixture
async def redis_client():
	"""Buat client Redis untuk pengujian asinkron."""
	url = os.getenv("TEST_REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
	client = Redis.from_url(url, decode_responses=True)
	try:
		yield client
	finally:
		await client.close()


@pytest_asyncio.fixture
async def test_client(async_db_pool, redis_client):
	"""Buat AsyncClient yang memakai aplikasi FastAPI secara langsung."""
	app.state.db_pool = async_db_pool
	app.state.redis = redis_client
	app.state.started_at = 0.0
	transport = ASGITransport(app=app)
	async with AsyncClient(transport=transport, base_url="http://testserver") as client:
		yield client
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4

import asyncpg
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aggregator import database
from aggregator.main import app
from aggregator.models import Event

DEFAULT_POSTGRES_DSN = os.getenv(
	"TEST_POSTGRES_DSN",
	os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@storage:5432/pubsub"),
)
DEFAULT_REDIS_URL = os.getenv(
	"TEST_REDIS_URL",
	os.getenv("REDIS_URL", "redis://broker:6379/15"),
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
