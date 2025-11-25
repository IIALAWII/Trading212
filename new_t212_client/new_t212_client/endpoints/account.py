"""Account-related endpoints."""
from __future__ import annotations

from typing import Any, Mapping

from ..client import T212Client


class AccountEndpoints:
    """Wrapper for account data operations."""

    def __init__(self, client: T212Client) -> None:
        self.client = client

    def fetch_cash(self) -> Mapping[str, Any]:
        """Pull `/equity/account/cash`."""

        response = self.client.get("/equity/account/cash", label="/equity/account/cash")
        return response.json()

    def fetch_info(self) -> Mapping[str, Any]:
        """Pull `/equity/account/info`."""

        response = self.client.get("/equity/account/info", label="/equity/account/info")
        return response.json()
