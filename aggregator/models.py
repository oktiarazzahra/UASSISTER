from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Event(BaseModel):
	model_config = ConfigDict(extra="forbid")

	topic: str
	event_id: str
	timestamp: str
	source: str
	payload: dict

	@field_validator("event_id")
	@classmethod
	def validate_event_id(cls, value: str) -> str:
		"""Validasi agar event_id harus UUID versi 4."""
		try:
			parsed = UUID(value)
		except ValueError as exc:
			raise ValueError("event_id harus UUID yang valid") from exc

		if parsed.version != 4:
			raise ValueError("event_id harus UUID v4")

		return value

	@field_validator("timestamp")
	@classmethod
	def validate_timestamp(cls, value: str) -> str:
		"""Validasi agar timestamp harus format ISO8601 yang valid."""
		normalized = value.replace("Z", "+00:00")
		try:
			datetime.fromisoformat(normalized)
		except ValueError as exc:
			raise ValueError("timestamp harus format ISO8601 yang valid") from exc

		return value


class EventBatch(BaseModel):
	model_config = ConfigDict(extra="forbid")

	events: list[Event] = Field(default_factory=list, max_length=1000)
