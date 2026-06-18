from __future__ import annotations

import asyncpg
import os

import pytest

from aggregator import database


@pytest.mark.asyncio
async def test_volume_persistence(async_db_pool, event_factory):
	"""Event yang sudah tersimpan harus tetap ada setelah pool ditutup dan dibuat ulang."""
	event = event_factory()
	first = await database.upsert_event(event, async_db_pool)
	assert first is True, "Insert awal harus sukses"
	await async_db_pool.close()
	dsn = os.getenv("TEST_POSTGRES_DSN", os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@localhost:5432/pubsub"))
	reopened = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
	try:
		async with reopened.acquire() as connection:
			count = await connection.fetchval("SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2", event.topic, event.event_id)
		assert count == 1, f"Event harus tetap ada di DB, ditemukan {count} baris"
		second = await database.upsert_event(event, reopened)
		assert second is False, "Event yang sama tidak boleh diproses ulang setelah reopen"
	finally:
		await reopened.close()from __future__ import annotations

import os

import pytest

from aggregator import database


@pytest.mark.asyncio
async def test_volume_persistence(event_factory):
	"""Pastikan data tetap ada setelah pool ditutup lalu dibuat ulang seperti simulasi restart."""
	dsn = os.getenv("TEST_POSTGRES_DSN", os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@storage:5432/pubsub"))
	first_pool = await database.init_db(dsn)
	event = event_factory(topic="sensor.reading")
	await database.upsert_event(event, first_pool)
	await first_pool.close()

	second_pool = await database.init_db(dsn)
	try:
		result = await database.upsert_event(event, second_pool)
		async with second_pool.acquire() as connection:
			count = await connection.fetchval(
				"SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
				event.topic,
				event.event_id,
			)
	finally:
		await second_pool.close()

	assert result is False, "Setelah pool dibuat ulang, event yang sama harus tetap dianggap duplikat"
	assert count == 1, f"Data harus tetap ada satu baris setelah restart, ditemukan {count}"
