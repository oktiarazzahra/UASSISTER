from __future__ import annotations

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
        count = await connection.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
            event.topic,
            event.event_id,
        )

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
async def test_process_event_atomik(async_db_pool, event_factory):
    """process_event harus menyimpan event dan memperbarui statistik secara atomik."""
    event = event_factory()
    result = await process_event(event, async_db_pool)

    assert result is True, "Event baru seharusnya diproses sebagai unik"

    async with async_db_pool.acquire() as connection:
        stats = await connection.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"
        )

    assert stats["received"] == 1, "received harus bertambah satu"
    assert stats["unique_processed"] == 1, "unique_processed harus bertambah satu"
    assert stats["duplicate_dropped"] == 0, "duplicate_dropped tidak boleh bertambah"
