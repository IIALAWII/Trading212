"""Incremental data collection service - only fetches NEW records."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import text

from ..client import T212Client
from ..config import Settings, get_settings
from ..endpoints.account import AccountEndpoints
from ..endpoints.history import HistoryEndpoints
from ..endpoints.portfolio import PortfolioEndpoints
from ..storage.sql_server import SqlServerRepository, create_sql_engine
from ..transformers import (
    build_account_cash_row,
    build_pending_order_rows,
    build_portfolio_rows,
    build_transaction_rows,
    extract_account_identity,
)

LOGGER = logging.getLogger(__name__)


class IncrementalCollectionService:
    """Collect current snapshots + only NEW historical data."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.engine = create_sql_engine(self.settings)
        self.repository = SqlServerRepository(self.engine)

    def run(self) -> Dict[str, Any]:
        """Execute incremental collection."""
        collection_time = datetime.now(timezone.utc)

        with T212Client(self.settings) as client:
            # Get account
            account_endpoints = AccountEndpoints(client)
            account_info = account_endpoints.fetch_info()
            account_id, _ = extract_account_identity(account_info)

            # Snapshots (always full refresh)
            cash_count = self._collect_cash_snapshot(account_endpoints, account_id, collection_time)
            portfolio_count = self._collect_portfolio_snapshot(client, account_id, collection_time)
            orders_count = self._collect_pending_orders(client, account_id, collection_time)

            # Incremental history
            transaction_count = self._collect_new_transactions(client, account_id)

        return {
            "account_id": account_id,
            "cash_snapshots": cash_count,
            "portfolio_positions": portfolio_count,
            "pending_orders": orders_count,
            "new_transactions": transaction_count,
        }

    def _collect_cash_snapshot(
        self,
        account_endpoints: AccountEndpoints,
        account_id: int,
        collection_time: datetime
    ) -> int:
        """Collect current cash snapshot."""
        LOGGER.info("[1/4] Collecting cash snapshot...")
        cash_payload = account_endpoints.fetch_cash()

        if cash_payload:
            cash_row = build_account_cash_row(account_id, cash_payload, collection_time)
            self.repository.insert_account_cash_snapshot(cash_row)
            LOGGER.info("✓ Cash snapshot saved")
            return 1
        return 0

    def _collect_portfolio_snapshot(
        self,
        client: T212Client,
        account_id: int,
        collection_time: datetime
    ) -> int:
        """Collect current portfolio positions."""
        LOGGER.info("[2/4] Collecting portfolio snapshot...")
        portfolio_endpoints = PortfolioEndpoints(client)
        portfolio_payload = portfolio_endpoints.fetch_portfolio()

        if portfolio_payload and isinstance(portfolio_payload, list):
            rows = build_portfolio_rows(account_id, portfolio_payload, collection_time)
            self.repository.insert_portfolio_snapshots(rows)
            LOGGER.info("✓ Portfolio: %d positions", len(rows))
            return len(rows)

        LOGGER.info("✓ No portfolio positions")
        return 0

    def _collect_pending_orders(
        self,
        client: T212Client,
        account_id: int,
        collection_time: datetime
    ) -> int:
        """Collect current pending orders."""
        LOGGER.info("[3/4] Collecting pending orders...")
        portfolio_endpoints = PortfolioEndpoints(client)
        orders_payload = portfolio_endpoints.fetch_orders()

        if orders_payload and isinstance(orders_payload, list):
            rows = build_pending_order_rows(account_id, orders_payload, collection_time)
            self.repository.insert_pending_order_snapshots(rows)
            LOGGER.info("✓ Pending orders: %d", len(rows))
            return len(rows)

        LOGGER.info("✓ No pending orders")
        return 0

    def _collect_new_transactions(self, client: T212Client, account_id: int) -> int:
        """Collect only NEW transactions since last collection."""
        LOGGER.info("[4/4] Collecting NEW transactions (incremental)...")

        # Get existing transaction references from DB
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT reference, transaction_type FROM core.transaction_history")
            ).fetchall()

        existing_refs = {(row[0], row[1]) for row in result} if result else set()

        if existing_refs:
            LOGGER.info("Found %d existing transactions in DB", len(existing_refs))
        else:
            LOGGER.info("No transactions in DB - will collect recent transactions")

        # Fetch new transactions with pagination
        history_endpoints = HistoryEndpoints(client)
        new_transactions = self._fetch_new_transactions(history_endpoints, existing_refs)

        if new_transactions:
            rows = build_transaction_rows(account_id, new_transactions)
            self.repository.insert_transaction_history(rows)
            LOGGER.info("✓ New transactions: %d", len(rows))
            return len(rows)

        LOGGER.info("✓ No new transactions")
        return 0

    def _fetch_new_transactions(
        self,
        history_endpoints: HistoryEndpoints,
        existing_refs: set
    ) -> List[Dict[str, Any]]:
        """Paginate through transactions, skip ones already in DB."""
        new_transactions = []
        params = {"limit": 50}
        page = 1
        max_pages = 5  # Rate limit safety

        while page <= max_pages:
            LOGGER.info("  Fetching page %d...", page)
            response = history_endpoints.fetch_transactions(params=params)

            if not response or not response.get('items'):
                LOGGER.info("  No more data")
                break

            page_items = response['items']
            LOGGER.info("  Page %d: %d items", page, len(page_items))

            # Filter out duplicates by checking (reference, type) combination
            new_count = 0
            for tx in page_items:
                ref = tx.get('reference')
                tx_type = tx.get('type')

                if (ref, tx_type) not in existing_refs:
                    new_transactions.append(tx)
                    existing_refs.add((ref, tx_type))  # Track to avoid dupes in same fetch
                    new_count += 1

            LOGGER.info("  Found %d new transactions on this page", new_count)

            # If no new transactions on this page, likely all caught up
            if new_count == 0:
                LOGGER.info("  No new transactions found - stopping pagination")
                break

            # Check for next page
            next_page = response.get('nextPagePath')
            if not next_page:
                break

            # Parse next page cursor
            if '?cursor=' in next_page:
                cursor = next_page.split('?cursor=')[1].split('&')[0]
                params['cursor'] = cursor

            page += 1

        if page > max_pages:
            LOGGER.warning("  Hit page limit - will continue next run")

        return new_transactions
