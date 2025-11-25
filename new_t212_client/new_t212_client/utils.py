"""Shared utility helpers for the Trading212 client."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import json


def dumps_payload(payload: Any) -> str:
    """Serialise payloads using a consistent compact format."""

    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def parse_api_datetime(value: str | None) -> datetime | None:
    """Parse ISO 8601 timestamps returned by the Trading212 API."""

    if not value:
        return None
    normalised = value.strip()
    if normalised.endswith("Z"):
        normalised = normalised[:-1] + "+00:00"
    if "+" not in normalised[10:]:  # no timezone info present
        normalised += "+00:00"
    try:
        return datetime.fromisoformat(normalised).astimezone(timezone.utc)
    except ValueError:
        return None


def to_decimal(value: Any) -> Decimal | None:
    """Convert numbers to Decimal for SQL precision."""

    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return None
