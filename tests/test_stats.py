from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_stats_initial(test_client):
	"""Setelah startup, statistik harus bernilai nol."""
	response = await test_client.get("/stats")
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	data = response.json()
	assert data["received"] == 0, "received awal harus 0"
	assert data["unique_processed"] == 0, "unique_processed awal harus 0"
	assert data["duplicate_dropped"] == 0, "duplicate_dropped awal harus 0"


@pytest.mark.asyncio
async def test_stats_update_after_publish(test_client, event_factory):
	"""Setelah publish campuran event unik dan duplikat, statistik harus akurat."""
	events = [event_factory(topic="order.created") for _ in range(7)]
	duplicates = [events[0], events[1], events[2]]
	for event in events + duplicates:
		await test_client.post("/publish", json=event.model_dump())
	response = await test_client.get("/stats")
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	data = response.json()
	assert data["received"] >= 10, "received harus mencerminkan total publish"
	assert data["unique_processed"] >= 7, "unique_processed minimal harus sama dengan event unik"
	assert data["received"] == data["unique_processed"] + data["duplicate_dropped"], "Statistik harus konsisten"


@pytest.mark.asyncio
async def test_health_endpoint(test_client):
	"""Endpoint health harus mengembalikan status ok saat dependency siap."""
	response = await test_client.get("/health")
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	data = response.json()
	assert data["status"] == "ok", "status health harus ok"from __future__ import annotations

import asyncpg
import pytest

from aggregator.dedup import process_event


@pytest.mark.asyncio
async def test_stats_initial(test_client):
	"""Pastikan stats awal setelah startup masih nol semua kecuali uptime."""
	response = await test_client.get("/stats")

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert body["received"] == 0, f"received awal harus 0, tetapi {body['received']}"
	assert body["unique_processed"] == 0, f"unique_processed awal harus 0, tetapi {body['unique_processed']}"
	assert body["duplicate_dropped"] == 0, f"duplicate_dropped awal harus 0, tetapi {body['duplicate_dropped']}"
	assert body["topics"] == [], f"topics awal harus kosong, tetapi {body['topics']}"


@pytest.mark.asyncio
async def test_stats_update_after_publish(test_client, async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan stats berubah benar setelah kombinasi event unik dan duplikat diproses."""
	events = [event_factory(topic="order.created") for _ in range(7)]
	events.extend(event_factory(topic="order.created", event_id=events[index].event_id) for index in range(3))
	await test_client.post("/publish", json={"events": [event.model_dump() for event in events]})
	for event in events:
		await process_event(event, async_db_pool)

	response = await test_client.get("/stats")
	body = response.json()

	assert body["received"] == 10, f"received harus 10, tetapi {body['received']}"
	assert body["unique_processed"] == 7, f"unique_processed harus 7, tetapi {body['unique_processed']}"
	assert body["duplicate_dropped"] == 3, f"duplicate_dropped harus 3, tetapi {body['duplicate_dropped']}"
	assert "order.created" in body["topics"], "Topic order.created harus muncul pada daftar topic"


@pytest.mark.asyncio
async def test_health_endpoint(test_client):
	"""Pastikan endpoint health mengembalikan status layanan yang siap dipakai."""
	response = await test_client.get("/health")

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert body["status"] == "ok", f"Status health harus ok, tetapi {body['status']}"
	assert body["db"] == "connected", f"DB harus connected, tetapi {body['db']}"
	assert body["broker"] == "connected", f"Broker harus connected, tetapi {body['broker']}"
