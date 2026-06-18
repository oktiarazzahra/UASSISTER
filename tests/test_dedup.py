from __future__ import annotations

import asyncio
import json

import pytest

from aggregator import database
from aggregator.dedup import process_event


@pytest.mark.asyncio
async def test_event_baru_diproses(async_db_pool, event_factory):
	"""Event baru harus diproses sebagai data unik."""
	event = event_factory()
	result = await database.upsert_event(event, async_db_pool)
	assert result is True, "Event baru seharusnya mengembalikan True"


@pytest.mark.asyncio
async def test_duplikat_diabaikan(async_db_pool, event_factory):
	"""Event yang sama dua kali harus ditandai duplikat pada percobaan kedua."""
	event = event_factory()
	first = await database.upsert_event(event, async_db_pool)
	second = await database.upsert_event(event, async_db_pool)
	assert first is True, "Insert pertama harus bernilai True"
	assert second is False, "Insert kedua harus bernilai False karena duplikat"


@pytest.mark.asyncio
async def test_100_duplikat(async_db_pool, event_factory):
	"""Mengirim event yang sama seratus kali hanya boleh tersimpan sekali."""
	event = event_factory()
	results = [await database.upsert_event(event, async_db_pool) for _ in range(100)]
	assert results.count(True) == 1, "Hanya satu insert pertama yang boleh True"
	assert results.count(False) == 99, "Sisa request harus ditolak sebagai duplikat"
	async with async_db_pool.acquire() as connection:
		count = await connection.fetchval("SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2", event.topic, event.event_id)
	assert count == 1, f"Event harus tersimpan satu kali, tetapi ditemukan {count} baris"


@pytest.mark.asyncio
async def test_duplikat_topic_berbeda(async_db_pool, event_factory):
	"""Event ID sama tetapi topic berbeda harus dianggap event berbeda."""
	event_a = event_factory(topic="user.login")
	event_b = event_factory(topic="order.created", event_id=event_a.event_id)
	first = await database.upsert_event(event_a, async_db_pool)
	second = await database.upsert_event(event_b, async_db_pool)
	assert first is True, "Event pertama harus diproses"
	assert second is True, "Topic berbeda seharusnya tidak dianggap duplikat"


@pytest.mark.asyncio
async def test_persistence_setelah_restart(async_db_pool, event_factory):
	"""Data harus tetap dedup setelah pool ditutup lalu dibuka lagi."""
	event = event_factory()
	first = await database.upsert_event(event, async_db_pool)
	assert first is True, "Insert pertama harus sukses"
	await async_db_pool.close()
	dsn = os.getenv("TEST_POSTGRES_DSN", os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@localhost:5432/pubsub"))
	reopened = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
	try:
		second = await database.upsert_event(event, reopened)
		assert second is False, "Setelah reopen, event yang sama harus tetap terdedup"
	finally:
		await reopened.close()


@pytest.mark.asyncio
async def test_process_event_atomik(async_db_pool, event_factory):
	"""process_event harus menyimpan event dan memperbarui statistik secara atomik."""
	event = event_factory()
	result = await process_event(event, async_db_pool)
	assert result is True, "Event baru seharusnya diproses sebagai unik"
	async with async_db_pool.acquire() as connection:
		stats = await connection.fetchrow("SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1")
	assert stats["received"] == 1, "received harus bertambah satu"
	assert stats["unique_processed"] == 1, "unique_processed harus bertambah satu"
	assert stats["duplicate_dropped"] == 0, "duplicate_dropped tidak boleh bertambah"from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import asyncpg
import pytest

from aggregator import database
from aggregator.models import Event


@pytest.mark.asyncio
async def test_event_baru_diproses(async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan event baru benar-benar diproses dan bernilai True saat insert pertama."""
	event = event_factory()
	result = await database.upsert_event(event, async_db_pool)

	assert result is True, "Event baru seharusnya mengembalikan True pada insert pertama"


@pytest.mark.asyncio
async def test_duplikat_diabaikan(async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan event yang sama dua kali diproses menghasilkan False pada percobaan kedua."""
	event = event_factory()
	first = await database.upsert_event(event, async_db_pool)
	second = await database.upsert_event(event, async_db_pool)

	assert first is True, "Insert pertama harus bernilai True untuk event baru"
	assert second is False, "Insert kedua harus bernilai False karena event sudah ada"


@pytest.mark.asyncio
async def test_100_duplikat(async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan seratus kiriman event yang sama hanya tersimpan satu kali di database."""
	event = event_factory()
	results = [await database.upsert_event(event, async_db_pool) for _ in range(100)]

	async with async_db_pool.acquire() as connection:
		count = await connection.fetchval(
			"SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
			event.topic,
			event.event_id,
		)

	assert results.count(True) == 1, "Hanya satu insert yang boleh bernilai True"
	assert count == 1, f"Seharusnya hanya ada 1 baris, tetapi ditemukan {count} baris"


@pytest.mark.asyncio
async def test_duplikat_topic_berbeda(async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan event_id yang sama tetapi topic berbeda tetap dianggap event berbeda."""
	event_id = str(uuid4())
	first = event_factory(topic="user.login", event_id=event_id)
	second = event_factory(topic="order.created", event_id=event_id)
	result_first = await database.upsert_event(first, async_db_pool)
	result_second = await database.upsert_event(second, async_db_pool)

	async with async_db_pool.acquire() as connection:
		count = await connection.fetchval(
			"SELECT COUNT(*) FROM processed_events WHERE event_id = $1",
			event_id,
		)

	assert result_first is True, "Event pertama harus tersimpan sebagai data baru"
	assert result_second is True, "Topic berbeda harus menghasilkan insert baru"
	assert count == 2, f"Dua topic berbeda seharusnya menghasilkan 2 baris, ditemukan {count}"


@pytest.mark.asyncio
async def test_persistence_setelah_restart(event_factory):
	"""Pastikan dedup tetap bekerja setelah pool ditutup lalu dibuat ulang."""
	dsn = os.getenv("TEST_POSTGRES_DSN", os.getenv("POSTGRES_DSN", "postgresql://pubsub:pubsub123@storage:5432/pubsub"))
	first_pool = await database.init_db(dsn)
	event = event_factory(topic="sensor.reading")
	try:
		first_result = await database.upsert_event(event, first_pool)
		await first_pool.close()
		second_pool = await database.init_db(dsn)
		try:
			second_result = await database.upsert_event(event, second_pool)
			async with second_pool.acquire() as connection:
				count = await connection.fetchval(
					"SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
					event.topic,
					event.event_id,
				)
		finally:
			await second_pool.close()
	finally:
		pass

	assert first_result is True, "Insert awal harus sukses"
	assert second_result is False, "Setelah restart, event yang sama harus tetap terdeteksi sebagai duplikat"
	assert count == 1, f"Event seharusnya tetap satu baris setelah restart, ditemukan {count}"
