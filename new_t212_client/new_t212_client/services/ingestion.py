"""Service orchestrating Trading212 data ingestion into SQL Server."""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Tuple
from urllib.parse import parse_qsl, urlparse

from tqdm import tqdm

from ..client import T212Client
from ..config import Settings, get_settings
from ..endpoints.account import AccountEndpoints
from ..endpoints.history import HistoryEndpoints
from ..endpoints.metadata import MetadataEndpoints
from ..endpoints.portfolio import PortfolioEndpoints
from ..storage.sql_server import SqlServerRepository, create_sql_engine
from ..transformers import (
    build_account_cash_row,
    build_dividend_rows,
    build_exchange_rows,
    build_instrument_rows,
    build_order_history_items,
    build_pending_order_rows,
    build_portfolio_rows,
    build_transaction_rows,
    extract_account_identity,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class IngestionSummary:
    """Keeps track of row counts produced during ingestion."""

    account_id: int | None = None
    account_cash_rows: int = 0
    portfolio_rows: int = 0
    pending_order_rows: int = 0
    order_history_rows: int = 0
    dividend_rows: int = 0
    transaction_rows: int = 0
    exchange_rows: int = 0
    working_schedule_rows: int = 0
    working_schedule_event_rows: int = 0
    instrument_rows: int = 0

    def as_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary."""
        return {
            "account_id": self.account_id,
            "account_cash_rows": self.account_cash_rows,
            "portfolio_rows": self.portfolio_rows,
            "pending_order_rows": self.pending_order_rows,
            "order_history_rows": self.order_history_rows,
            "dividend_rows": self.dividend_rows,
            "transaction_rows": self.transaction_rows,
            "exchange_rows": self.exchange_rows,
            "working_schedule_rows": self.working_schedule_rows,
            "working_schedule_event_rows": self.working_schedule_event_rows,
            "instrument_rows": self.instrument_rows,
        }


class IngestionService:
    """Coordinate API calls, transformations, and persistence."""

    # Configuration constants
    PAGE_LIMIT = 50
    HISTORY_RATE_LIMIT_DELAY = 12  # seconds between history API calls
    RATE_LIMIT_RETRY_DELAY = 30  # seconds to wait after 429 error

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        repository: SqlServerRepository | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.engine = repository.engine if repository else create_sql_engine(self.settings)
        self.repository = repository or SqlServerRepository(self.engine)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_full_snapshot(self) -> Dict[str, Any]:
        """Execute the full pull of current state and history snapshots."""
        summary = IngestionSummary()
        correlation_id = uuid.uuid4().hex

        with T212Client(self.settings) as client:
            account_id = self._ingest_account_state(client, summary, correlation_id)
            if account_id is None:
                raise RuntimeError(
                    "Account information could not be retrieved; aborting ingestion."
                )

            self._ingest_portfolio_state(client, summary, account_id, correlation_id)
            self._ingest_history(client, summary, account_id, correlation_id)
            self._ingest_metadata(client, summary, correlation_id)

        LOGGER.info("Ingestion summary: %s", summary.as_dict())
        return summary.as_dict()

    # ------------------------------------------------------------------
    # Account state
    # ------------------------------------------------------------------
    def _ingest_account_state(
        self,
        client: T212Client,
        summary: IngestionSummary,
        correlation_id: str,
    ) -> int | None:
        LOGGER.info("Fetching account information...")
        account_endpoints = AccountEndpoints(client)

        account_payload = account_endpoints.fetch_info()
        if not account_payload:
            LOGGER.error("Account info payload empty")
            return None

        account_endpoint = self._format_endpoint("equity/account/info")
        info_captured_at = self.repository.record_raw_payload(
            account_endpoint,
            account_payload,
            correlation_id=correlation_id,
        )
        account_id, currency_code = extract_account_identity(account_payload)
        summary.account_id = account_id
        self.repository.upsert_account_profile(account_id, currency_code, info_captured_at)
        LOGGER.info("Account %d (%s) profile updated", account_id, currency_code)

        LOGGER.info("Fetching account cash balance...")
        cash_payload = account_endpoints.fetch_cash()
        cash_endpoint = self._format_endpoint("equity/account/cash")
        cash_captured_at = self.repository.record_raw_payload(
            cash_endpoint,
            cash_payload,
            account_id=account_id,
            correlation_id=correlation_id,
        )
        cash_row = build_account_cash_row(account_id, cash_payload, cash_captured_at)
        self.repository.insert_account_cash_snapshot(cash_row)
        summary.account_cash_rows = 1
        LOGGER.info(
            "Cash snapshot inserted: Total=%.2f, Free=%.2f, Invested=%.2f",
            cash_row.get("total_equity", 0),
            cash_row.get("free_amount", 0),
            cash_row.get("invested_amount", 0),
        )
        return account_id

    # ------------------------------------------------------------------
    # Portfolio snapshot
    # ------------------------------------------------------------------
    def _ingest_portfolio_state(
        self,
        client: T212Client,
        summary: IngestionSummary,
        account_id: int,
        correlation_id: str,
    ) -> None:
        LOGGER.info("Fetching portfolio positions...")
        portfolio_endpoints = PortfolioEndpoints(client)

        positions_payload = portfolio_endpoints.fetch_portfolio() or []
        portfolio_endpoint = self._format_endpoint("equity/portfolio")
        portfolio_captured_at = self.repository.record_raw_payload(
            portfolio_endpoint,
            positions_payload,
            account_id=account_id,
            correlation_id=correlation_id,
        )
        portfolio_rows = build_portfolio_rows(account_id, positions_payload, portfolio_captured_at)
        if portfolio_rows:
            self.repository.insert_portfolio_snapshots(portfolio_rows)
            summary.portfolio_rows = len(portfolio_rows)
            LOGGER.info("Inserted %d portfolio position snapshots", len(portfolio_rows))
        else:
            LOGGER.info("No open positions in portfolio")

        LOGGER.info("Fetching pending orders...")
        orders_payload = portfolio_endpoints.fetch_orders() or []
        orders_endpoint = self._format_endpoint("equity/orders")
        orders_captured_at = self.repository.record_raw_payload(
            orders_endpoint,
            orders_payload,
            account_id=account_id,
            correlation_id=correlation_id,
        )
        order_rows = build_pending_order_rows(account_id, orders_payload, orders_captured_at)
        if order_rows:
            self.repository.insert_pending_order_snapshots(order_rows)
            summary.pending_order_rows = len(order_rows)
            LOGGER.info("Inserted %d pending order snapshots", len(order_rows))
        else:
            LOGGER.info("No pending orders")

    # ------------------------------------------------------------------
    # Historical pulls
    # ------------------------------------------------------------------
    def _ingest_history(
        self,
        client: T212Client,
        summary: IngestionSummary,
        account_id: int,
        correlation_id: str,
    ) -> None:
        LOGGER.info("HISTORICAL DATA INGESTION (Account %d)", account_id)
        history_endpoints = HistoryEndpoints(client)

        # Historical orders
        LOGGER.info("[1/3] Historical Orders - Starting pagination...")
        orders = self._collect_paginated_items(
            client,
            base_path="equity/history/orders",
            first_page_loader=lambda: history_endpoints.fetch_orders(
                params={"limit": self.PAGE_LIMIT}
            ),
            account_id=account_id,
            correlation_id=correlation_id,
        )
        LOGGER.info("[1/3] Fetched %d historical orders", len(orders))

        if orders:
            LOGGER.info("[1/3] Transforming and inserting orders...")
            order_bundles = build_order_history_items(account_id, orders)
            inserted_count = 0
            duplicate_count = 0

            with tqdm(
                total=len(order_bundles),
                desc="  Inserting orders",
                unit="order",
                leave=False,
            ) as pbar:
                for bundle in order_bundles:
                    try:
                        self.repository.insert_order_history(bundle.order, bundle.taxes)
                        inserted_count += 1
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                            duplicate_count += 1
                        else:
                            LOGGER.error(
                                "  Failed to insert order %s: %s",
                                bundle.order.get("order_id"),
                                str(e),
                            )
                    pbar.update(1)

            summary.order_history_rows = inserted_count
            LOGGER.info(
                "[1/3] Inserted %d new orders, skipped %d duplicates",
                inserted_count,
                duplicate_count,
            )

        # Dividends
        LOGGER.info("[2/3] Dividend History - Starting pagination...")
        dividends = self._collect_paginated_items(
            client,
            base_path="history/dividends",
            first_page_loader=lambda: history_endpoints.fetch_dividends(
                params={"limit": self.PAGE_LIMIT}
            ),
            account_id=account_id,
            correlation_id=correlation_id,
        )
        LOGGER.info("[2/3] Fetched %d dividend records", len(dividends))

        dividend_rows = build_dividend_rows(account_id, dividends)
        if dividend_rows:
            self.repository.insert_dividend_history(dividend_rows)
            summary.dividend_rows = len(dividend_rows)
            LOGGER.info("[2/3] Inserted %d dividend records", len(dividend_rows))

        # Transactions
        LOGGER.info("[3/3] Transaction History - Starting pagination...")
        transactions = self._collect_paginated_items(
            client,
            base_path="history/transactions",
            first_page_loader=lambda: history_endpoints.fetch_transactions(
                params={"limit": self.PAGE_LIMIT}
            ),
            account_id=account_id,
            correlation_id=correlation_id,
        )
        LOGGER.info("[3/3] Fetched %d transaction records", len(transactions))

        transaction_rows = build_transaction_rows(account_id, transactions)
        if transaction_rows:
            self.repository.insert_transaction_history(transaction_rows)
            summary.transaction_rows = len(transaction_rows)
            LOGGER.info("[3/3] Inserted %d transaction records", len(transaction_rows))

        LOGGER.info("Historical data ingestion complete")

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------
    def _ingest_metadata(
        self,
        client: T212Client,
        summary: IngestionSummary,
        correlation_id: str,
    ) -> None:
        LOGGER.info("METADATA INGESTION")
        metadata_endpoints = MetadataEndpoints(client)

        # Exchanges
        LOGGER.info("[1/2] Fetching exchanges and working schedules...")
        exchanges_payload = metadata_endpoints.fetch_exchanges() or []
        exchanges_endpoint = self._format_endpoint("equity/metadata/exchanges")
        self.repository.record_raw_payload(
            exchanges_endpoint,
            exchanges_payload,
            correlation_id=correlation_id,
        )

        exchanges, schedules, events = build_exchange_rows(exchanges_payload)
        if exchanges:
            self.repository.upsert_exchanges(exchanges, schedules, events)
            summary.exchange_rows = len(exchanges)
            summary.working_schedule_rows = len(schedules)
            summary.working_schedule_event_rows = len(events)
            LOGGER.info(
                "[1/2] Upserted %d exchanges, %d schedules, %d events",
                len(exchanges),
                len(schedules),
                len(events),
            )

        # Instruments
        LOGGER.info("[2/2] Fetching tradable instruments...")
        instruments_payload = metadata_endpoints.fetch_instruments() or []
        instruments_endpoint = self._format_endpoint("equity/metadata/instruments")
        self.repository.record_raw_payload(
            instruments_endpoint,
            instruments_payload,
            correlation_id=correlation_id,
        )

        instruments = build_instrument_rows(instruments_payload)
        if instruments:
            self.repository.upsert_instruments(instruments)
            summary.instrument_rows = len(instruments)
            LOGGER.info("[2/2] Upserted %d instruments", len(instruments))

        LOGGER.info("Metadata ingestion complete")

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------
    def _collect_paginated_items(
        self,
        client: T212Client,
        *,
        base_path: str,
        first_page_loader,
        account_id: int,
        correlation_id: str,
    ) -> List[Mapping[str, Any]]:
        """Fetch all pages for a paginated response."""
        collected: List[Mapping[str, Any]] = []
        endpoint_label = self._format_endpoint(base_path)

        # Fetch first page
        first_payload = first_page_loader()
        self.repository.record_raw_payload(
            endpoint_label,
            first_payload,
            account_id=account_id,
            correlation_id=correlation_id,
        )
        page_items = first_payload.get("items", [])
        collected.extend(page_items)
        next_path = first_payload.get("nextPagePath")

        if not next_path:
            LOGGER.info("  Retrieved %d items (single page)", len(collected))
            return collected

        # Multiple pages
        page_num = 1
        seen_cursors = {next_path}
        pbar = tqdm(
            desc=f"  Paginating {base_path}",
            unit="page",
            initial=1,
            bar_format="{desc}: {n} pages | {rate_fmt}",
            leave=False,
        )

        while next_path:
            page_num += 1

            # Rate limiting for history endpoints
            if "history" in base_path:
                time.sleep(self.HISTORY_RATE_LIMIT_DELAY)

            request_path, params = self._normalise_next_page_path(base_path, next_path)

            try:
                response = client.get(request_path, params=params, label=endpoint_label)
                page_payload = response.json()
            except Exception as e:  # pylint: disable=broad-exception-caught
                error_str = str(e).lower()

                if "429" in error_str or "too many requests" in error_str:
                    LOGGER.warning("  Rate limit hit! Waiting %ds...", self.RATE_LIMIT_RETRY_DELAY)
                    time.sleep(self.RATE_LIMIT_RETRY_DELAY)
                    continue

                if "transactions" in base_path and ("400" in error_str or "404" in error_str):
                    LOGGER.info("  End of transaction data reached")
                    pbar.close()
                    break

                LOGGER.error("  Failed to fetch page %d: %s", page_num, str(e))
                LOGGER.warning("  Stopping pagination. Collected %d items so far.", len(collected))
                pbar.close()
                break

            self.repository.record_raw_payload(
                endpoint_label,
                page_payload,
                account_id=account_id,
                correlation_id=correlation_id,
            )
            page_items = page_payload.get("items", [])
            collected.extend(page_items)
            next_path = page_payload.get("nextPagePath")

            # Detect cursor loops
            if next_path:
                if next_path in seen_cursors:
                    LOGGER.warning("  Pagination loop detected. Stopping.")
                    pbar.close()
                    break
                seen_cursors.add(next_path)

            pbar.update(1)

        pbar.close()
        LOGGER.info("  Pagination complete: %d pages, %d total items", page_num, len(collected))
        return collected

    def _normalise_next_page_path(
        self,
        base_path: str,
        next_page_path: str,
    ) -> Tuple[str, Dict[str, Any] | None]:
        """Convert API `nextPagePath` strings into client target + params."""
        # Handle query-string-only paths
        if next_page_path.startswith("?"):
            next_page_path = base_path + next_page_path
        elif not next_page_path.startswith(("http://", "https://", "/")):
            # Bare query string without ?
            next_page_path = f"{base_path}?{next_page_path}"

        # Parse URL
        parsed = urlparse(next_page_path)

        # Extract relative path
        relative_path = parsed.path.lstrip("/")
        if relative_path.startswith("api/v0/"):
            relative_path = relative_path[len("api/v0/") :]
        relative_path = relative_path or base_path

        # Parse query params
        params = dict(parse_qsl(parsed.query)) if parsed.query else None

        # Remove 'time' parameter for transactions (API requirement)
        if params and "transactions" in base_path and "time" in params:
            del params["time"]

        return relative_path, params

    @staticmethod
    def _format_endpoint(relative_path: str) -> str:
        trimmed = relative_path.lstrip("/")
        if not trimmed.startswith("api/v0/"):
            trimmed = f"api/v0/{trimmed}"
        return f"/{trimmed}"
