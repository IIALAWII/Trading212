"""Historical data endpoints."""
from __future__ import annotations

from typing import Any, Mapping

from ..client import T212Client


class HistoryEndpoints:
    """Fetch paginated historical resources."""

    def __init__(self, client: T212Client) -> None:
        self.client = client

    def fetch_orders(self, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        '''Fetch paginated historical orders.'''
        response = self.client.get("/equity/history/orders",
        params=params, label="/equity/history/orders")
        return response.json()

    def fetch_dividends(self, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        '''Fetch paginated historical dividends.'''
        response = self.client.get("/history/dividends", params=params, label="/history/dividends")
        return response.json()

    def fetch_transactions(self, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        '''Fetch paginated historical transactions.'''
        response = self.client.get("/history/transactions",
        params=params, label="/history/transactions")
        return response.json()
