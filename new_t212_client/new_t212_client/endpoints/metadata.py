"""Metadata endpoint helpers."""
from __future__ import annotations

from typing import Any, Mapping

from ..client import T212Client


class MetadataEndpoints:
    """Access instrument metadata."""

    def __init__(self, client: T212Client) -> None:
        self.client = client

    def fetch_exchanges(self) -> list[Mapping[str, Any]]:
        response = self.client.get("/equity/metadata/exchanges", label="/equity/metadata/exchanges")
        return response.json()

    def fetch_instruments(self) -> list[Mapping[str, Any]]:
        response = self.client.get("/equity/metadata/instruments", label="/equity/metadata/instruments")
        return response.json()
