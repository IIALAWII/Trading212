"""Authentication helpers."""
from __future__ import annotations

from typing import Mapping

from .config import Settings, get_settings


def build_auth_headers(settings: Settings | None = None) -> Mapping[str, str]:
    """Return a header dict containing the Basic auth token."""

    config = settings or get_settings()
    return {"Authorization": config.auth_header, "Accept": "application/json"}
