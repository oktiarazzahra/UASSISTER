from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

import asyncpg
from redis.asyncio import Redis

from . import database
from .dedup import process_event
from .models import Event

logger = logging.getLogger(__name__)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

GROUP_NAME = "aggregator-workers"
STREAM_PREFIX = "events:"


async def _ensure_group(redis_client: Redis, stream_name: str) -> None:
	"""Buat consumer group jika stream dan grup belum ada."""
	try:
		await redis_client.xgroup_create(stream_name, GROUP_NAME, id="0", mkstream=True)
	except Exception as exc:
		if "BUSYGROUP" not in str(exc):
			raise


async def _discover_streams(redis_client: Redis) -> list[str]:
	"""Cari semua stream yang cocok dengan pola events:* untuk diproses worker."""
	keys = await redis_client.keys(f"{STREAM_PREFIX}*")
	return [key for key in keys if key.startswith(STREAM_PREFIX)]


async def _process_message(
	redis_client: Redis,
	db_pool: asyncpg.Pool,
	stream_name: str,
	message_id: str,
	fields: dict[str, Any],
) -> None:
	"""Decode payload JSON, proses event, lalu ACK jika berhasil."""
	payload_raw = fields.get("event")
	if payload_raw is None:
		raise ValueError("Payload event kosong")

	if isinstance(payload_raw, bytes):
		payload_raw = payload_raw.decode("utf-8")

	event = Event.model_validate(json.loads(payload_raw))
	await process_event(event, db_pool)
	await redis_client.xack(stream_name, GROUP_NAME, message_id)


async def consume_loop(
	worker_name: str,
	redis_client: Redis,
	db_pool: asyncpg.Pool,
) -> None:
	"""Subscribe ke stream Redis, proses message, dan retry dengan backoff."""
	backoff_seconds = 1

	while True:
		try:
			streams = await _discover_streams(redis_client)
			for stream_name in streams:
				await _ensure_group(redis_client, stream_name)

			if not streams:
				await asyncio.sleep(1)
				continue

			response = await redis_client.xreadgroup(
				GROUP_NAME,
				worker_name,
				streams={stream: ">" for stream in streams},
				count=10,
				block=1000,
			)

			if not response:
				continue

			backoff_seconds = 1
			for stream_name, messages in response:
				for message_id, fields in messages:
					for attempt in range(3):
						try:
							await _process_message(redis_client, db_pool, stream_name, message_id, fields)
							break
						except Exception as exc:
							if attempt == 2:
								logger.exception(
									"Gagal memproses event di stream %s setelah 3 percobaan: %s",
									stream_name,
									exc,
								)
								raise
							await asyncio.sleep(backoff_seconds)
							backoff_seconds = min(backoff_seconds * 2, 8)
		except asyncio.CancelledError:
			raise
		except Exception as exc:
			logger.exception("Worker %s error: %s", worker_name, exc)
			await asyncio.sleep(backoff_seconds)
			backoff_seconds = min(backoff_seconds * 2, 8)


async def run_workers(num_workers: int = 4) -> None:
	"""Jalankan beberapa worker secara paralel dengan asyncio.gather()."""
	redis_url = os.getenv("REDIS_URL", "redis://broker:6379/0")
	database_url = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
	if not database_url:
		raise ValueError("DSN Postgres belum diset")

	redis_client = Redis.from_url(redis_url, decode_responses=True)
	db_pool = await database.init_db(database_url)

	try:
		tasks = [
			consume_loop(f"worker-{index + 1}", redis_client, db_pool)
			for index in range(num_workers)
		]
		await asyncio.gather(*tasks)
	finally:
		await redis_client.close()
		await db_pool.close()


if __name__ == "__main__":
	asyncio.run(run_workers(int(os.getenv("WORKER_COUNT", "4"))))
