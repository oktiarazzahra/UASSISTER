from __future__ import annotations

import asyncio
import json
from collections import Counter
from uuid import uuid4

import asyncpg
import pytest

from aggregator.dedup import process_event
from aggregator.models import Event


@pytest.mark.asyncio
async def test_concurrent_same_event(async_db_pool: asyncpg.Pool, event_factory):
    """Pastikan sepuluh coroutine memproses event yang sama tanpa membuat duplikasi baris."""
    event = event_factory(topic="user.login")
    results = await asyncio.gather(*(process_event(event, async_db_pool) for _ in range(10)))

    async with async_db_pool.acquire() as connection:
        count = await connection.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
            event.topic,
            event.event_id,
        )

    assert sum(results) == 1, f"Hanya satu coroutine yang boleh berhasil, tetapi hasilnya {results}"
    assert count == 1, f"Seharusnya hanya ada 1 row, tetapi ditemukan {count}"


@pytest.mark.asyncio
async def test_no_race_condition(async_db_pool: asyncpg.Pool, event_factory):
    """Pastikan beban 1000 event dengan 200 duplikat tetap menghasilkan statistik yang konsisten."""
    unique_events = [event_factory(topic="sensor.reading") for _ in range(800)]
    duplicate_events = [
        event_factory(topic="sensor.reading", event_id=unique_events[index].event_id)
        for index in range(200)
    ]
    all_events = unique_events + duplicate_events

    chunks = [all_events[index:index + 20] for index in range(0, len(all_events), 20)]
    await asyncio.gather(
        *(asyncio.gather(*(process_event(event, async_db_pool) for event in chunk)) for chunk in chunks)
    )

    async with async_db_pool.acquire() as connection:
        stats = await connection.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"
        )
        row_count = await connection.fetchval("SELECT COUNT(*) FROM processed_events")

    assert stats is not None, "Baris stats harus tersedia setelah load test"
    assert stats["received"] == 1000, f"received seharusnya 1000, tetapi {stats['received']}"
    assert stats["unique_processed"] == 800, f"unique_processed seharusnya 800, tetapi {stats['unique_processed']}"
    assert stats["duplicate_dropped"] == 200, f"duplicate_dropped seharusnya 200, tetapi {stats['duplicate_dropped']}"
    assert row_count == 800, f"Hanya 800 event unik yang boleh tersimpan, tetapi {row_count}"


@pytest.mark.asyncio
async def test_stats_atomic(async_db_pool: asyncpg.Pool, event_factory):
    """Pastikan statistik selalu memenuhi invariant received = unique_processed + duplicate_dropped."""
    events = [event_factory(topic="payment.processed") for _ in range(60)]
    for index in range(15):
        events.append(event_factory(topic="payment.processed", event_id=events[index].event_id))

    await asyncio.gather(*(process_event(event, async_db_pool) for event in events))

    async with async_db_pool.acquire() as connection:
        stats = await connection.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"
        )

    assert stats is not None, "Stats harus tersedia setelah pemrosesan event"
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"], (
        f"Invariant rusak: received={stats['received']}, unique={stats['unique_processed']}, duplicate={stats['duplicate_dropped']}"
    )


@pytest.mark.asyncio
async def test_multi_worker_no_double_process(async_db_pool: asyncpg.Pool, redis_client, event_factory):
    """Pastikan empat worker pada consumer group yang sama tidak memproses message dua kali."""
    stream_name = "events:queue"
    group_name = "aggregator-workers"
    count_total = 100
    for _ in range(count_total):
        event = event_factory(topic="system.error")
        await redis_client.xadd(stream_name, {"event": event.model_dump_json()})

    try:
        await redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    processed_counter = 0
    processed_lock = asyncio.Lock()

    async def worker_loop(worker_name: str) -> None:
        nonlocal processed_counter
        while True:
            async with processed_lock:
                if processed_counter >= count_total:
                    return
            response = await redis_client.xreadgroup(
                group_name,
                worker_name,
                streams={stream_name: ">"},
                count=5,
                block=100,
            )
            if not response:
                return
            for _, messages in response:
                for message_id, fields in messages:
                    event_payload = json.loads(fields["event"])
                    event = Event.model_validate(event_payload)
                    await process_event(event, async_db_pool)
                    await redis_client.xack(stream_name, group_name, message_id)
                    async with processed_lock:
                        processed_counter += 1
                        if processed_counter >= count_total:
                            return

    await asyncio.gather(
        worker_loop("worker-1"),
        worker_loop("worker-2"),
        worker_loop("worker-3"),
        worker_loop("worker-4"),
    )

    async with async_db_pool.acquire() as connection:
        row_count = await connection.fetchval("SELECT COUNT(*) FROM processed_events WHERE topic = $1", "system.error")

    assert row_count == count_total, f"Setiap message hanya boleh diproses satu kali, tetapi ada {row_count} baris"
