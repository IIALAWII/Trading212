"""SQL Server persistence helpers and repository implementation."""
from __future__ import annotations

import hashlib
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from ..config import Settings, get_settings
from ..utils import dumps_payload

LOGGER = logging.getLogger(__name__)


def build_connection_string(settings: Settings | None = None) -> str:
    """Return a SQLAlchemy connection string based on environment variables."""

    config = settings or get_settings()
    if config.sqlserver_username:
        creds = f"UID={config.sqlserver_username};PWD={config.sqlserver_password};"
    else:
        creds = "Trusted_Connection=yes;"
    trust = "TrustServerCertificate=yes;" if config.sqlserver_trust_cert else ""
    odbc_str = (
        f"Driver={{{config.sqlserver_driver}}};"
        f"Server={config.sqlserver_server};"
        f"Database={config.sqlserver_database};"
        f"Encrypt={config.sqlserver_encrypt};"
        f"{trust}"
        f"{creds}"
    )
    quoted = urllib.parse.quote_plus(odbc_str)
    return f"mssql+pyodbc:///?odbc_connect={quoted}"


def create_sql_engine(settings: Settings | None = None) -> Engine:
    """Instantiate a SQLAlchemy engine with sensible defaults."""

    return create_engine(build_connection_string(settings), fast_executemany=True, future=True)


class SqlServerRepository:
    """Thin repository encapsulating SQL Server persistence concerns."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # ------------------------------------------------------------------
    # Staging helpers
    # ------------------------------------------------------------------
    def record_raw_payload(
        self,
        endpoint: str,
        payload: Any,
        account_id: int | None = None,
        correlation_id: str | None = None,
        captured_at: datetime | None = None,
    ) -> datetime:
        """Store a raw API response in the staging schema."""

        captured = captured_at or datetime.now(timezone.utc)
        payload_json = dumps_payload(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).digest()

        stmt = text(
            """
            INSERT INTO staging.raw_api_payload
                (endpoint, account_id, captured_at_utc, correlation_id, payload_hash, payload_json)
            VALUES (:endpoint, :account_id, :captured_at_utc, :correlation_id, :payload_hash, :payload_json)
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "endpoint": endpoint,
                    "account_id": account_id,
                    "captured_at_utc": captured,
                    "correlation_id": correlation_id,
                    "payload_hash": payload_hash,
                    "payload_json": payload_json,
                },
            )
        return captured

    # ------------------------------------------------------------------
    # Account profile
    # ------------------------------------------------------------------
    def upsert_account_profile(self, account_id: int, currency_code: str, seen_at: datetime) -> None:
        """Insert or update the account profile dimension."""

        stmt = text(
            """
            MERGE core.account_profile AS target
            USING (VALUES (:account_id, :currency_code, :seen_at)) AS source(account_id, currency_code, seen_at)
            ON target.account_id = source.account_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.currency_code = source.currency_code,
                    target.last_seen_at = source.seen_at
            WHEN NOT MATCHED THEN
                INSERT (account_id, currency_code, first_seen_at, last_seen_at)
                VALUES (source.account_id, source.currency_code, source.seen_at, source.seen_at);
            """
        )

        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "account_id": account_id,
                    "currency_code": currency_code,
                    "seen_at": seen_at,
                },
            )

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    def insert_account_cash_snapshot(self, row: Mapping[str, Any]) -> None:
        stmt = text(
            """
            INSERT INTO core.account_cash_snapshot (
                account_id, captured_at_utc, blocked_amount, free_amount, invested_amount,
                pie_cash_amount, unrealised_ppl, realised_result, total_equity,
                source_system, payload_json
            )
            VALUES (
                :account_id, :captured_at_utc, :blocked_amount, :free_amount, :invested_amount,
                :pie_cash_amount, :unrealised_ppl, :realised_result, :total_equity,
                :source_system, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            conn.execute(stmt, dict(row))

    def insert_portfolio_snapshots(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Replace portfolio snapshots - clear old data and insert new snapshot."""
        
        # Clear existing portfolio snapshots for this account
        delete_stmt = text("DELETE FROM core.portfolio_position_snapshot WHERE account_id = :account_id")
        
        insert_stmt = text(
            """
            INSERT INTO core.portfolio_position_snapshot (
                account_id, captured_at_utc, ticker, quantity, average_price,
                current_price, ppl_amount, fx_ppl_amount, pie_quantity,
                max_buy_quantity, max_sell_quantity, initial_fill_date,
                frontend_origin, payload_json
            )
            VALUES (
                :account_id, :captured_at_utc, :ticker, :quantity, :average_price,
                :current_price, :ppl_amount, :fx_ppl_amount, :pie_quantity,
                :max_buy_quantity, :max_sell_quantity, :initial_fill_date,
                :frontend_origin, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            rows_list = list(rows)  # Convert to list so we can use it twice
            if rows_list:
                # Clear existing snapshots for this account
                account_id = rows_list[0].get("account_id")
                conn.execute(delete_stmt, {"account_id": account_id})
                
                # Insert new snapshots
                for row in rows_list:
                    conn.execute(insert_stmt, dict(row))

    def insert_pending_order_snapshots(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Replace pending order snapshots - clear old data and insert new snapshot."""
        
        # Clear existing pending orders for this account
        delete_stmt = text("DELETE FROM core.pending_order_snapshot WHERE account_id = :account_id")
        
        insert_stmt = text(
            """
            INSERT INTO core.pending_order_snapshot (
                account_id, captured_at_utc, order_id, ticker, order_type,
                order_status, strategy, quantity, value_amount, limit_price,
                stop_price, extended_hours, filled_quantity, filled_value,
                creation_time_utc, payload_json
            )
            VALUES (
                :account_id, :captured_at_utc, :order_id, :ticker, :order_type,
                :order_status, :strategy, :quantity, :value_amount, :limit_price,
                :stop_price, :extended_hours, :filled_quantity, :filled_value,
                :creation_time_utc, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            rows_list = list(rows)  # Convert to list so we can use it twice
            if rows_list:
                # Clear existing pending orders for this account
                account_id = rows_list[0].get("account_id")
                conn.execute(delete_stmt, {"account_id": account_id})
                
                # Insert new pending orders
                for row in rows_list:
                    conn.execute(insert_stmt, dict(row))

    def insert_pie_allocation_snapshots(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Replace pie allocation snapshots - clear old data and insert new snapshot."""
        
        # Clear existing pie allocations for this account
        delete_stmt = text("DELETE FROM core.pie_allocation_snapshot WHERE account_id = :account_id")
        
        insert_stmt = text(
            """
            INSERT INTO core.pie_allocation_snapshot (
                account_id, captured_at_utc, pie_id, ticker, 
                target_weight_pct, actual_weight_pct, quantity, payload_json
            )
            VALUES (
                :account_id, :captured_at_utc, :pie_id, :ticker,
                :target_weight_pct, :actual_weight_pct, :quantity, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            rows_list = list(rows)  # Convert to list so we can use it twice
            if rows_list:
                # Clear existing pie allocations for this account
                account_id = rows_list[0].get("account_id")
                conn.execute(delete_stmt, {"account_id": account_id})
                
                # Insert new pie allocations
                for row in rows_list:
                    conn.execute(insert_stmt, dict(row))

    # ------------------------------------------------------------------
    # Historical facts
    # ------------------------------------------------------------------
    def insert_order_history(self, order_row: Mapping[str, Any], taxes: Iterable[Mapping[str, Any]] | None = None) -> None:
        stmt = text(
            """
            INSERT INTO core.order_history (
                account_id, order_id, parent_order_id, ticker, order_type,
                order_status, time_validity, executor, extended_hours,
                ordered_quantity, ordered_value, filled_quantity, filled_value,
                fill_price, fill_cost, fill_result, fill_type, fill_id,
                limit_price, stop_price, placed_at_utc, executed_at_utc,
                modified_at_utc, payload_json
            )
            OUTPUT inserted.order_history_id
            VALUES (
                :account_id, :order_id, :parent_order_id, :ticker, :order_type,
                :order_status, :time_validity, :executor, :extended_hours,
                :ordered_quantity, :ordered_value, :filled_quantity, :filled_value,
                :fill_price, :fill_cost, :fill_result, :fill_type, :fill_id,
                :limit_price, :stop_price, :placed_at_utc, :executed_at_utc,
                :modified_at_utc, :payload_json
            )
            """
        )

        select_stmt = text(
            """
            SELECT order_history_id
            FROM core.order_history
            WHERE order_id = :order_id
              AND ((fill_id IS NULL AND :fill_id IS NULL) OR fill_id = :fill_id)
            """
        )

        tax_stmt = text(
            """
            INSERT INTO core.order_history_tax (
                order_history_id, fill_id, tax_name, tax_quantity, time_charged_utc, payload_json
            )
            VALUES (:order_history_id, :fill_id, :tax_name, :tax_quantity, :time_charged_utc, :payload_json)
            """
        )

        with self.engine.begin() as conn:
            order_history_id: int
            try:
                result = conn.execute(stmt, dict(order_row))
                order_history_id = int(result.scalar_one())
            except IntegrityError:
                LOGGER.debug(
                    "Order history duplicate detected for order %s / fill %s",
                    order_row.get("order_id"),
                    order_row.get("fill_id"),
                )
                result = conn.execute(select_stmt, {
                    "order_id": order_row.get("order_id"),
                    "fill_id": order_row.get("fill_id"),
                })
                existing = result.scalar_one()
                order_history_id = int(existing)

            if not taxes:
                return

            for tax in taxes:
                try:
                    conn.execute(
                        tax_stmt,
                        {
                            "order_history_id": order_history_id,
                            "fill_id": tax.get("fill_id"),
                            "tax_name": tax.get("tax_name"),
                            "tax_quantity": tax.get("tax_quantity"),
                            "time_charged_utc": tax.get("time_charged_utc"),
                            "payload_json": tax.get("payload_json"),
                        },
                    )
                except IntegrityError:
                    LOGGER.debug(
                        "Duplicate tax row skipped for order_history_id=%s tax=%s",
                        order_history_id,
                        tax.get("tax_name"),
                    )

    def insert_dividend_history(self, rows: Iterable[Mapping[str, Any]]) -> None:
        stmt = text(
            """
            INSERT INTO core.dividend_history (
                account_id, reference, ticker, dividend_type, quantity,
                gross_amount_per_share, amount_account_ccy, amount_eur,
                paid_on_utc, payload_json
            )
            VALUES (
                :account_id, :reference, :ticker, :dividend_type, :quantity,
                :gross_amount_per_share, :amount_account_ccy, :amount_eur,
                :paid_on_utc, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            for row in rows:
                try:
                    conn.execute(stmt, dict(row))
                except IntegrityError:
                    LOGGER.debug("Dividend history duplicate skipped for reference %s", row.get("reference"))

    def insert_transaction_history(self, rows: Iterable[Mapping[str, Any]]) -> None:
        stmt = text(
            """
            INSERT INTO core.transaction_history (
                account_id, reference, transaction_type, amount_account_ccy,
                occurred_at_utc, payload_json
            )
            VALUES (
                :account_id, :reference, :transaction_type, :amount_account_ccy,
                :occurred_at_utc, :payload_json
            )
            """
        )

        with self.engine.begin() as conn:
            for row in rows:
                try:
                    conn.execute(stmt, dict(row))
                except IntegrityError:
                    LOGGER.debug(
                        "Transaction history duplicate skipped for reference %s",
                        row.get("reference"),
                    )

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------
    def upsert_exchanges(
        self,
        exchanges: Iterable[Mapping[str, Any]],
        working_schedules: Iterable[Mapping[str, Any]],
        schedule_events: Iterable[Mapping[str, Any]],
    ) -> None:
        exchange_stmt = text(
            """
            MERGE core.exchange AS target
            USING (VALUES (:exchange_id, :exchange_name, :payload_json))
                AS source(exchange_id, exchange_name, payload_json)
            ON target.exchange_id = source.exchange_id
            WHEN MATCHED THEN
                UPDATE SET exchange_name = source.exchange_name, payload_json = source.payload_json
            WHEN NOT MATCHED THEN
                INSERT (exchange_id, exchange_name, payload_json)
                VALUES (source.exchange_id, source.exchange_name, source.payload_json);
            """
        )

        schedule_stmt = text(
            """
            MERGE core.working_schedule AS target
            USING (VALUES (:working_schedule_id, :exchange_id, :payload_json))
                AS source(working_schedule_id, exchange_id, payload_json)
            ON target.working_schedule_id = source.working_schedule_id
            WHEN MATCHED THEN
                UPDATE SET exchange_id = source.exchange_id, payload_json = source.payload_json
            WHEN NOT MATCHED THEN
                INSERT (working_schedule_id, exchange_id, payload_json)
                VALUES (source.working_schedule_id, source.exchange_id, source.payload_json);
            """
        )

        event_stmt = text(
            """
            INSERT INTO core.working_schedule_event (
                working_schedule_id, event_type, event_time_utc, payload_json
            )
            VALUES (:working_schedule_id, :event_type, :event_time_utc, :payload_json)
            """
        )

        with self.engine.begin() as conn:
            for exchange in exchanges:
                conn.execute(exchange_stmt, dict(exchange))

            for schedule in working_schedules:
                conn.execute(schedule_stmt, dict(schedule))

            for event in schedule_events:
                try:
                    conn.execute(event_stmt, dict(event))
                except IntegrityError:
                    LOGGER.debug(
                        "Schedule event duplicate skipped for schedule %s at %s",
                        event.get("working_schedule_id"),
                        event.get("event_time_utc"),
                    )

    def upsert_instruments(self, rows: Iterable[Mapping[str, Any]]) -> None:
        stmt = text(
            """
            MERGE core.instrument AS target
            USING (VALUES (
                :ticker, :isin, :name, :short_name, :currency_code,
                :instrument_type, :working_schedule_id, :max_open_quantity,
                :added_on_utc, :payload_json
            )) AS source(
                ticker, isin, name, short_name, currency_code,
                instrument_type, working_schedule_id, max_open_quantity,
                added_on_utc, payload_json
            )
            ON target.ticker = source.ticker
            WHEN MATCHED THEN
                UPDATE SET
                    isin = source.isin,
                    name = source.name,
                    short_name = source.short_name,
                    currency_code = source.currency_code,
                    instrument_type = source.instrument_type,
                    working_schedule_id = source.working_schedule_id,
                    max_open_quantity = source.max_open_quantity,
                    added_on_utc = source.added_on_utc,
                    payload_json = source.payload_json
            WHEN NOT MATCHED THEN
                INSERT (
                    ticker, isin, name, short_name, currency_code,
                    instrument_type, working_schedule_id, max_open_quantity,
                    added_on_utc, payload_json
                )
                VALUES (
                    source.ticker, source.isin, source.name, source.short_name, source.currency_code,
                    source.instrument_type, source.working_schedule_id, source.max_open_quantity,
                    source.added_on_utc, source.payload_json
                );
            """
        )

        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(stmt, dict(row))

