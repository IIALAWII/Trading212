"""Portfolio endpoint helpers."""
from __future__ import annotations

from typing import Any, Mapping

from ..client import T212Client


class PortfolioEndpoints:
    """Fetch portfolio and order state."""

    def __init__(self, client: T212Client) -> None:
        self.client = client

    def fetch_portfolio(self) -> list[Mapping[str, Any]]:
        '''Fetch current portfolio holdings.'''
        response = self.client.get("/equity/portfolio", label="/equity/portfolio")
        return response.json()

    def fetch_orders(self) -> list[Mapping[str, Any]]:
        '''Fetch current pending orders.'''
        response = self.client.get("/equity/orders", label="/equity/orders")
        return response.json()

    def fetch_pies(self) -> list[Mapping[str, Any]]:
        '''Fetch current pies.'''
        response = self.client.get("/equity/pies", label="/equity/pies")
        return response.json()

    def fetch_pie_details(self, pie_id: int) -> Mapping[str, Any]:
        '''Fetch details for a specific pie.'''
        response = self.client.get(f"/equity/pies/{pie_id}", label=f"/equity/pies/{pie_id}")
        return response.json()
