from __future__ import annotations

import logging

import asyncpg

from . import database
from .models import Event

logger = logging.getLogger(__name__)


# Isolation Level: READ COMMITTED
# Alasan: INSERT ON CONFLICT DO NOTHING bersifat atomik di level PostgreSQL.
# Unique constraint (topic, event_id) menjamin hanya satu transaksi yang berhasil
# meski ada concurrent insert. READ COMMITTED cukup karena tidak ada
# phantom read risk pada use case ini — dedup ditangani constraint, bukan
# application-level SELECT dulu baru INSERT.
# Trade-off: SERIALIZABLE lebih aman untuk write-skew, tapi overhead lebih tinggi
# dan tidak dibutuhkan untuk pola idempotent upsert ini.
# Memproses satu event secara atomik agar insert dan update statistik selalu konsisten.
async def process_event(event: Event, db_pool: asyncpg.Pool) -> bool:
	"""Proses event, simpan ke database, dan perbarui statistik dalam satu transaksi."""

	async with db_pool.acquire() as connection:
		async with connection.transaction(isolation="read_committed"):
			is_new = await database.upsert_event(event, connection)
			await database.update_stats(is_new, connection)

	if is_new:
		logger.info("Event baru diproses: %s", event.event_id)
	else:
		logger.info("Duplikat diabaikan: %s", event.event_id)

	return is_new
