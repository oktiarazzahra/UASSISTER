from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis


TOPICS = [
	"user.login",
	"order.created",
	"payment.processed",
	"system.error",
	"sensor.reading",
]


@dataclass(slots=True)
class PublishedEvent:
	"""Simpan event yang pernah dikirim agar bisa dipakai untuk duplikat sengaja."""
	data: dict[str, Any]


def _build_payload(topic: str) -> dict[str, Any]:
	"""Buat payload acak yang sesuai dengan topic event."""
	if topic == "user.login":
		return {
			"user_id": random.randint(1, 100000),
			"ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
			"success": random.choice([True, False]),
		}
	if topic == "order.created":
		return {
			"order_id": random.randint(100000, 999999),
			"amount": round(random.uniform(10.0, 5000.0), 2),
			"currency": random.choice(["IDR", "USD", "SGD"]),
		}
	if topic == "payment.processed":
		return {
			"payment_id": random.randint(100000, 999999),
			"amount": round(random.uniform(5.0, 3000.0), 2),
			"method": random.choice(["card", "transfer", "ewallet"]),
			"status": random.choice(["paid", "failed", "pending"]),
		}
	if topic == "system.error":
		return {
			"code": random.choice([400, 401, 403, 500, 503]),
			"message": random.choice([
				"Timeout saat memanggil layanan",
				"Database tidak tersedia",
				"Payload tidak valid",
			]),
			"severity": random.choice(["low", "medium", "high"]),
		}
	return {
		"sensor_id": random.randint(1, 1000),
		"value": round(random.uniform(-20.0, 60.0), 2),
		"unit": random.choice(["C", "kPa", "%", "lux"]),
	}


def _build_event(hostname: str) -> dict[str, Any]:
	"""Buat satu event baru dengan struktur yang konsisten untuk semua topic."""
	topic = random.choice(TOPICS)
	return {
		"topic": topic,
		"event_id": str(uuid4()),
		"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "+00:00",
		"source": f"publisher-{hostname}",
		"payload": _build_payload(topic),
	}


async def publish_forever() -> None:
	"""Kirim event terus-menerus ke Redis stream dengan sebagian duplikat sengaja."""
	redis_url = os.getenv("REDIS_URL", "redis://broker:6379/0")
	events_per_second = float(os.getenv("EVENTS_PER_SECOND", "100"))
	interval = 1.0 / events_per_second if events_per_second > 0 else 0.01
	hostname = socket.gethostname().split(".")[0]
	redis_client = Redis.from_url(redis_url, decode_responses=True)
	buffer: list[PublishedEvent] = []
	published = 0
	duplicates = 0
	logger.info("Publisher dimulai, rate %s event/detik", events_per_second)

	try:
		while True:
			if buffer and random.random() < 0.3:
				event = dict(random.choice(buffer).data)
				duplicates += 1
			else:
				event = _build_event(hostname)
				buffer.append(PublishedEvent(data=event))
				if len(buffer) > 100:
					buffer.pop(0)

			await redis_client.xadd(f"events:{event['topic']}", {"event": json.dumps(event)})
			published += 1

			if published % 1000 == 0:
				print(f"Sudah kirim {published} event, {duplicates} duplikat", flush=True)

			await asyncio.sleep(interval)
	finally:
		await redis_client.close()


if __name__ == "__main__":
	asyncio.run(publish_forever())
