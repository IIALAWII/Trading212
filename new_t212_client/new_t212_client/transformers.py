"""Transform Trading212 API payloads into relational-friendly rows."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, List, Mapping, Sequence

from .utils import dumps_payload, parse_api_datetime, to_decimal

SOURCE_SYSTEM = "api"


@dataclass(frozen=True)
class OrderWithTaxes:
    """Container for an order history row and associated tax records."""

    order: Mapping[str, Any]
    taxes: Sequence[Mapping[str, Any]]


def extract_account_identity(payload: Mapping[str, Any]) -> tuple[int, str]:
    """Return the account id and upper-case currency code from the payload."""

    account_id = int(payload["id"])
    currency_code = str(payload.get("currencyCode", "")).upper()
    return account_id, currency_code


def build_account_cash_row(
    account_id: int, payload: Mapping[str, Any], captured_at: datetime
) -> Mapping[str, Any]:
    """Map account cash metrics into the curated snapshot schema."""

    return {
        "account_id": account_id,
        "captured_at_utc": captured_at,
        "blocked_amount": to_decimal(payload.get("blocked")),
        "free_amount": to_decimal(payload.get("free")),
        "invested_amount": to_decimal(payload.get("invested")),
        "pie_cash_amount": to_decimal(payload.get("pieCash")),
        "unrealised_ppl": to_decimal(payload.get("ppl")),
        "realised_result": to_decimal(payload.get("result")),
        "total_equity": to_decimal(payload.get("total")),
        "source_system": SOURCE_SYSTEM,
        "payload_json": dumps_payload(payload),
    }


def build_portfolio_rows(
    account_id: int,
    positions: Iterable[Mapping[str, Any]],
    captured_at: datetime,
) -> List[Mapping[str, Any]]:
    """Normalise each open position into the portfolio snapshot table."""

    rows: List[Mapping[str, Any]] = []
    for position in positions:
        rows.append(
            {
                "account_id": account_id,
                "captured_at_utc": captured_at,
                "ticker": position.get("ticker"),
                "quantity": to_decimal(position.get("quantity")),
                "average_price": to_decimal(position.get("averagePrice")),
                "current_price": to_decimal(position.get("currentPrice")),
                "ppl_amount": to_decimal(position.get("ppl")),
                "fx_ppl_amount": to_decimal(position.get("fxPpl")),
                "pie_quantity": to_decimal(position.get("pieQuantity")),
                "max_buy_quantity": to_decimal(position.get("maxBuy")),
                "max_sell_quantity": to_decimal(position.get("maxSell")),
                "initial_fill_date": parse_api_datetime(position.get("initialFillDate")),
                "frontend_origin": position.get("frontend"),
                "payload_json": dumps_payload(position),
            }
        )
    return rows


def build_pie_allocation_rows(
    account_id: int,
    pie_details: Iterable[Mapping[str, Any]],
    captured_at: datetime,
) -> List[Mapping[str, Any]]:
    """Map pie allocation data into snapshot rows."""

    rows: List[Mapping[str, Any]] = []
    for pie_detail in pie_details:
        pie_id = pie_detail.get("settings", {}).get("id")
        instruments = pie_detail.get("instruments", [])

        for instrument in instruments:
            rows.append(
                {
                    "account_id": account_id,
                    "captured_at_utc": captured_at,
                    "pie_id": str(pie_id) if pie_id else None,
                    "ticker": instrument.get("ticker"),
                    "target_weight_pct": to_decimal(instrument.get("expectedShare")),
                    "actual_weight_pct": to_decimal(instrument.get("currentShare")),
                    "quantity": to_decimal(instrument.get("ownedQuantity")),
                    "payload_json": dumps_payload(instrument),
                }
            )
    return rows


def build_pending_order_rows(
    account_id: int,
    orders: Iterable[Mapping[str, Any]],
    captured_at: datetime,
) -> List[Mapping[str, Any]]:
    """Convert current pending orders into snapshot rows."""

    rows: List[Mapping[str, Any]] = []
    for order in orders:
        rows.append(
            {
                "account_id": account_id,
                "captured_at_utc": captured_at,
                "order_id": int(order["id"]),  # Back to int
                "ticker": order.get("ticker"),
                "order_type": order.get("type"),
                "order_status": order.get("status"),
                "strategy": order.get("strategy"),
                "quantity": to_decimal(order.get("quantity")),
                "value_amount": to_decimal(order.get("value")),
                "limit_price": to_decimal(order.get("limitPrice")),
                "stop_price": to_decimal(order.get("stopPrice")),
                "extended_hours": bool(order.get("extendedHours")),
                "filled_quantity": to_decimal(order.get("filledQuantity")),
                "filled_value": to_decimal(order.get("filledValue")),
                "creation_time_utc": parse_api_datetime(order.get("creationTime")),
                "payload_json": dumps_payload(order),
            }
        )
    return rows


def build_order_history_items(
    account_id: int, items: Iterable[Mapping[str, Any]]
) -> List[OrderWithTaxes]:
    """Transform historical order payloads into database-ready structures."""

    bundles: List[OrderWithTaxes] = []
    for item in items:
        order_row = {
            "account_id": account_id,
            "order_id": int(item["id"]),  # Back to int - the original working format
            "parent_order_id": (
                int(item["parentOrder"]) if item.get("parentOrder") is not None else None
            ),
            "ticker": item.get("ticker"),
            "order_type": item.get("type"),
            "order_status": item.get("status"),
            "time_validity": item.get("timeValidity"),
            "executor": item.get("executor"),
            "extended_hours": bool(item.get("extendedHours")),
            "ordered_quantity": to_decimal(item.get("orderedQuantity")),
            "ordered_value": to_decimal(item.get("orderedValue")),
            "filled_quantity": to_decimal(item.get("filledQuantity")),
            "filled_value": to_decimal(item.get("filledValue")),
            "fill_price": to_decimal(item.get("fillPrice")),
            "fill_cost": to_decimal(item.get("fillCost")),
            "fill_result": to_decimal(item.get("fillResult")),
            "fill_type": item.get("fillType"),
            "fill_id": int(item["fillId"]) if item.get("fillId") is not None else None,
            "limit_price": to_decimal(item.get("limitPrice")),
            "stop_price": to_decimal(item.get("stopPrice")),
            "placed_at_utc": parse_api_datetime(item.get("dateCreated")),
            "executed_at_utc": parse_api_datetime(item.get("dateExecuted")),
            "modified_at_utc": parse_api_datetime(item.get("dateModified")),
            "payload_json": dumps_payload(item),
        }

        tax_rows: List[Mapping[str, Any]] = []
        for tax in item.get("taxes", []):
            tax_rows.append(
                {
                    "fill_id": tax.get("fillId"),
                    "tax_name": tax.get("name"),
                    "tax_quantity": to_decimal(tax.get("quantity")) or Decimal("0"),
                    "time_charged_utc": parse_api_datetime(tax.get("timeCharged")),
                    "payload_json": dumps_payload(tax),
                }
            )

        bundles.append(OrderWithTaxes(order=order_row, taxes=tax_rows))

    return bundles


def build_dividend_rows(
    account_id: int, items: Iterable[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    """Map dividend history items into curated rows."""

    rows: List[Mapping[str, Any]] = []
    for item in items:
        rows.append(
            {
                "account_id": account_id,
                "reference": item.get("reference"),
                "ticker": item.get("ticker"),
                "dividend_type": item.get("type"),
                "quantity": to_decimal(item.get("quantity")),
                "gross_amount_per_share": to_decimal(item.get("grossAmountPerShare")),
                "amount_account_ccy": to_decimal(item.get("amount")) or Decimal("0"),
                "amount_eur": to_decimal(item.get("amountInEuro")),
                "paid_on_utc": (
                    parse_api_datetime(item.get("paidOn")) or datetime.now(timezone.utc)
                ),
                "payload_json": dumps_payload(item),
            }
        )
    return rows


def build_transaction_rows(
    account_id: int, items: Iterable[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    """Map cash transactions into curated rows."""

    rows: List[Mapping[str, Any]] = []
    for item in items:
        rows.append(
            {
                "account_id": account_id,
                "reference": item.get("reference"),
                "transaction_type": item.get("type"),
                "amount_account_ccy": to_decimal(item.get("amount")) or Decimal("0"),
                "occurred_at_utc": (
                    parse_api_datetime(item.get("dateTime")) or datetime.now(timezone.utc)
                ),
                "payload_json": dumps_payload(item),
            }
        )
    return rows


def build_exchange_rows(
    exchanges_payload: Iterable[Mapping[str, Any]],
) -> tuple[List[Mapping[str, Any]], List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    """Create rows for exchanges, schedules, and schedule events."""

    exchanges: List[Mapping[str, Any]] = []
    schedules: List[Mapping[str, Any]] = []
    events: List[Mapping[str, Any]] = []

    for exchange in exchanges_payload:
        exchange_id = int(exchange["id"])
        exchanges.append(
            {
                "exchange_id": exchange_id,
                "exchange_name": exchange.get("name"),
                "payload_json": dumps_payload(exchange),
            }
        )

        for schedule in exchange.get("workingSchedules", []):
            schedule_id = int(schedule["id"])
            schedules.append(
                {
                    "working_schedule_id": schedule_id,
                    "exchange_id": exchange_id,
                    "payload_json": dumps_payload(schedule),
                }
            )

            for event in schedule.get("timeEvents", []):
                parsed_time = parse_api_datetime(event.get("date"))
                if parsed_time is None:
                    continue
                events.append(
                    {
                        "working_schedule_id": schedule_id,
                        "event_type": event.get("type"),
                        "event_time_utc": parsed_time,
                        "payload_json": dumps_payload(event),
                    }
                )

    return exchanges, schedules, events


def build_instrument_rows(instruments: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """Transform metadata instruments for upsert into the core.instrument table."""

    rows: List[Mapping[str, Any]] = []
    for instrument in instruments:
        added_on = parse_api_datetime(instrument.get("addedOn"))
        rows.append(
            {
                "ticker": instrument.get("ticker"),
                "isin": instrument.get("isin"),
                "name": instrument.get("name"),
                "short_name": instrument.get("shortName"),
                "currency_code": (instrument.get("currencyCode") or "").upper(),
                "instrument_type": instrument.get("type"),
                "working_schedule_id": int(instrument["workingScheduleId"])
                if instrument.get("workingScheduleId") is not None
                else None,
                "max_open_quantity": to_decimal(instrument.get("maxOpenQuantity")),
                "added_on_utc": added_on,
                "payload_json": dumps_payload(instrument),
            }
        )
    return rows
