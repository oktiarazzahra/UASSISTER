from __future__ import annotations

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
    response = await test_client.post(
        "/publish",
        json={"events": [event.model_dump() for event in events]},
    )

    assert response.status_code == 200, f"Status kode harus 200, bukan {response.status_code}"
    body = response.json()
    assert body["received"] == 10, "Batch 10 event harus dihitung sebagai 10"

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
    assert len(data) == 10, f"Pagination harus mengembalikan 10 data, tetapi ditemukan {len(data)}"
