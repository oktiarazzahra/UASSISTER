from __future__ import annotations

import json

import pytest


@pytest.mark.asyncio
async def test_publish_single_event(test_client, event_factory, redis_client):
	"""POST /publish harus menerima satu event dan menaruhnya ke Redis stream."""
	event = event_factory()
	response = await test_client.post("/publish", json=event.model_dump())
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	body = response.json()
	assert body["received"] == 1, "Endpoint harus melaporkan 1 event diterima"
	entries = await redis_client.xrange(f"events:{event.topic}")
	assert len(entries) >= 1, "Redis stream harus berisi event yang baru dikirim"


@pytest.mark.asyncio
async def test_publish_batch(test_client, event_factory, redis_client):
	"""POST /publish harus menerima batch event dan mengirim semuanya ke broker."""
	events = [event_factory(topic="order.created") for _ in range(10)]
	response = await test_client.post("/publish", json={"events": [event.model_dump() for event in events]})
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	assert response.json()["received"] == 10, "Batch 10 event harus dihitung sebagai 10"
	entries = await redis_client.xrange("events:order.created")
	assert len(entries) >= 10, "Semua event batch harus masuk stream Redis"


@pytest.mark.asyncio
async def test_publish_invalid_schema(test_client, event_factory):
	"""Payload tanpa event_id harus ditolak dengan status 422."""
	event = event_factory().model_dump()
	event.pop("event_id")
	response = await test_client.post("/publish", json=event)
	assert response.status_code == 422, f"Status kode harus 422, bukan {response.status_code}"


@pytest.mark.asyncio
async def test_get_events_by_topic(test_client, event_factory):
	"""GET /events harus bisa memfilter event berdasarkan topic."""
	for _ in range(5):
		await test_client.post("/publish", json=event_factory(topic="user.login").model_dump())
	for _ in range(3):
		await test_client.post("/publish", json=event_factory(topic="order.created").model_dump())
	response = await test_client.get("/events", params={"topic": "user.login"})
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	data = response.json()
	assert len(data) == 5, f"Harus kembali 5 event user.login, tetapi ditemukan {len(data)}"
	assert all(item["topic"] == "user.login" for item in data), "Semua event harus berasal dari topic user.login"


@pytest.mark.asyncio
async def test_get_events_pagination(test_client, event_factory):
	"""GET /events harus mendukung limit dan offset dengan benar."""
	for _ in range(20):
		await test_client.post("/publish", json=event_factory(topic="sensor.reading").model_dump())
	response = await test_client.get("/events", params={"limit": 10, "offset": 10})
	assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
	data = response.json()
	assert len(data) == 10, f"Pagination harus mengembalikan 10 data, tetapi ditemukan {len(data)}"from __future__ import annotations

import asyncio
from collections import defaultdict
from uuid import UUID, uuid4

import asyncpg
import pytest

from aggregator.dedup import process_event
from aggregator.models import Event, EventBatch


@pytest.mark.asyncio
async def test_publish_single_event(test_client, redis_client, event_factory):
	"""Pastikan endpoint publish menerima satu event dan menulisnya ke Redis stream."""
	event = event_factory(topic="user.login")
	response = await test_client.post("/publish", json=event.model_dump())

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert body["received"] == 1, f"Jumlah event diterima seharusnya 1, tetapi {body['received']}"
	assert await redis_client.xlen(f"events:{event.topic}") == 1, "Event harus masuk ke Redis stream"


@pytest.mark.asyncio
async def test_publish_batch(test_client, redis_client, event_factory):
	"""Pastikan endpoint publish menerima batch dan seluruh event masuk ke stream."""
	events = [event_factory(topic="order.created") for _ in range(10)]
	batch = EventBatch(events=events)
	response = await test_client.post("/publish", json=batch.model_dump())

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert body["received"] == 10, f"Batch 10 event seharusnya diterima semua, tetapi {body['received']}"
	assert await redis_client.xlen("events:order.created") == 10, "Sepuluh event harus tersimpan di stream"


@pytest.mark.asyncio
async def test_publish_invalid_schema(test_client):
	"""Pastikan payload tanpa event_id ditolak dengan status 422 oleh Pydantic."""
	response = await test_client.post(
		"/publish",
		json={
			"topic": "user.login",
			"timestamp": "2026-06-17T00:00:00Z",
			"source": "publisher-test",
			"payload": {"user_id": 1},
		},
	)

	assert response.status_code == 422, f"Payload invalid harus menghasilkan 422, tetapi {response.status_code}"


@pytest.mark.asyncio
async def test_get_events_by_topic(test_client, async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan endpoint events bisa memfilter data berdasarkan topic tertentu."""
	for _ in range(5):
		await process_event(event_factory(topic="topic.a"), async_db_pool)
	for _ in range(3):
		await process_event(event_factory(topic="topic.b"), async_db_pool)

	response = await test_client.get("/events", params={"topic": "topic.a"})

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert len(body) == 5, f"Topic A seharusnya mengembalikan 5 event, tetapi {len(body)}"
	assert all(item["topic"] == "topic.a" for item in body), "Semua event harus berasal dari topic yang difilter"


@pytest.mark.asyncio
async def test_get_events_pagination(test_client, async_db_pool: asyncpg.Pool, event_factory):
	"""Pastikan pagination limit dan offset bekerja saat mengambil event dari database."""
	for _ in range(20):
		await process_event(event_factory(topic="topic.pagination"), async_db_pool)

	response = await test_client.get("/events", params={"limit": 10, "offset": 10})

	assert response.status_code == 200, f"Status code seharusnya 200, tetapi {response.status_code}"
	body = response.json()
	assert len(body) == 10, f"Pagination seharusnya mengembalikan 10 event, tetapi {len(body)}"
