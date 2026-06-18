from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager

import asyncpg

from .models import Event

logger = logging.getLogger(__name__)

_POOL: asyncpg.Pool | None = None


def _get_database_url(dsn: str | None = None) -> str:
	"""Ambil DSN database dari argumen atau environment."""
	database_url = dsn or os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
	if not database_url:
		raise ValueError("DSN Postgres belum diset")
	return database_url


@asynccontextmanager
async def _resolve_connection(
	db_pool: asyncpg.Pool | asyncpg.Connection | None,
):
	"""Ambil koneksi aktif dari pool atau gunakan koneksi yang sudah ada."""
	if db_pool is None:
		if _POOL is None:
			raise ValueError("Pool database belum diinisialisasi")
		async with _POOL.acquire() as connection:
			yield connection
		return

	if isinstance(db_pool, asyncpg.Connection):
		yield db_pool
		return

	async with db_pool.acquire() as connection:
		yield connection


# Membuat pool dan skema database saat aplikasi pertama kali berjalan.
async def init_db(dsn: str | None = None) -> asyncpg.Pool:
	"""Inisialisasi pool koneksi dan tabel yang dibutuhkan aplikasi."""
	global _POOL

	database_url = _get_database_url(dsn)
	pool = await asyncpg.create_pool(database_url, min_size=5, max_size=20)

	async with pool.acquire() as connection:
		# Isolation level READ COMMITTED cukup untuk dedup berbasis unique key.
		async with connection.transaction(isolation="read_committed"):
			await connection.execute(
				"""
				CREATE TABLE IF NOT EXISTS processed_events (
					id SERIAL,
					topic VARCHAR NOT NULL,
					event_id VARCHAR NOT NULL,
					source VARCHAR NOT NULL,
					payload JSONB NOT NULL,
					received_at TIMESTAMP NOT NULL DEFAULT NOW(),
					PRIMARY KEY (topic, event_id)
				)
				"""
			)
			logger.info("Tabel processed_events berhasil dibuat")
			await connection.execute(
				"""
				CREATE TABLE IF NOT EXISTS stats (
					id INT PRIMARY KEY DEFAULT 1,
					received BIGINT NOT NULL DEFAULT 0,
					unique_processed BIGINT NOT NULL DEFAULT 0,
					duplicate_dropped BIGINT NOT NULL DEFAULT 0,
					started_at TIMESTAMP NOT NULL DEFAULT NOW()
				)
				"""
			)
			logger.info("Tabel stats berhasil dibuat")
			await connection.execute(
				"""
				INSERT INTO stats (id, received, unique_processed, duplicate_dropped, started_at)
				VALUES (1, 0, 0, 0, NOW())
				ON CONFLICT (id) DO NOTHING
				"""
			)

	_POOL = pool
	return pool


# Menyimpan event baru secara idempotent dan mengembalikan status duplikat.
async def upsert_event(
	event: Event,
	db_pool: asyncpg.Pool | asyncpg.Connection | None = None,
) -> bool:
	"""Simpan event ke tabel processed_events dan tandai apakah event baru."""

	async with _resolve_connection(db_pool) as connection:
		# Kalau koneksi sudah berada dalam transaksi dari caller, jangan buka transaksi baru.
		# READ COMMITTED dipakai karena dedup bergantung pada unique constraint per baris.
		if db_pool is None or isinstance(db_pool, asyncpg.Pool):
			async with connection.transaction(isolation="read_committed"):
				result = await connection.execute(
					"""
					INSERT INTO processed_events (topic, event_id, source, payload, received_at)
					VALUES ($1, $2, $3, $4::jsonb, NOW())
					ON CONFLICT (topic, event_id) DO NOTHING
					""",
					event.topic,
					event.event_id,
					event.source,
					json.dumps(event.payload),
				)
		else:
			result = await connection.execute(
				"""
				INSERT INTO processed_events (topic, event_id, source, payload, received_at)
				VALUES ($1, $2, $3, $4::jsonb, NOW())
				ON CONFLICT (topic, event_id) DO NOTHING
				""",
				event.topic,
				event.event_id,
				event.source,
				json.dumps(event.payload),
			)

	return result == "INSERT 0 1"


# Memperbarui statistik agregasi berdasarkan hasil pemrosesan event.
async def update_stats(
	is_new: bool,
	db_pool: asyncpg.Pool | asyncpg.Connection | None = None,
) -> None:
	"""Perbarui baris stats dengan satu statement atomic agar total event dan duplikat selalu sinkron."""

	async with _resolve_connection(db_pool) as connection:
		# Fungsi ini harus memakai koneksi/transaksi yang sudah dibuka caller, supaya
		# update stats berjalan di transaksi yang sama dengan INSERT di upsert_event().
		# Dengan satu UPDATE atomic, PostgreSQL mengubah nilai secara in-place di
		# bawah row lock yang sama, sehingga tidak ada lost-update saat banyak worker
		# menaikkan counter secara paralel.
		await connection.execute(
			"""
			UPDATE stats
			SET
			  received = received + 1,
			  unique_processed = unique_processed + CASE WHEN $1 THEN 1 ELSE 0 END,
			  duplicate_dropped = duplicate_dropped + CASE WHEN NOT $1 THEN 1 ELSE 0 END
			WHERE id = 1
			""",
			is_new,
		)
