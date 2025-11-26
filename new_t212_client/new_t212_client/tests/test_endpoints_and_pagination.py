"""Test all endpoints and pagination logic."""
from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path
from typing import Any

# Setup path before imports
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PACKAGE_ROOT.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Now import after path is set
from new_t212_client.client import T212Client  # noqa: E402
from new_t212_client.config import get_settings  # noqa: E402
from new_t212_client.endpoints.account import AccountEndpoints  # noqa: E402
from new_t212_client.endpoints.history import HistoryEndpoints  # noqa: E402
from new_t212_client.endpoints.metadata import MetadataEndpoints  # noqa: E402
from new_t212_client.endpoints.portfolio import PortfolioEndpoints  # noqa: E402
from new_t212_client.logging_config import configure_logging  # noqa: E402


def test_account_endpoints(client: T212Client) -> dict[str, Any]:
    """Test account info and cash endpoints."""
    print("\n" + "=" * 80)
    print("TESTING ACCOUNT ENDPOINTS")
    print("=" * 80)

    endpoints = AccountEndpoints(client)

    # Test account info
    print("\n[1/2] Fetching account info...")
    account_info = endpoints.fetch_info()
    print(f"✓ Account ID: {account_info.get('id')}")
    print(f"✓ Currency: {account_info.get('currencyCode')}")

    # Test cash balance
    print("\n[2/2] Fetching cash balance...")
    cash = endpoints.fetch_cash()
    print(f"✓ Total: {cash.get('total')}")
    print(f"✓ Free: {cash.get('free')}")
    print(f"✓ Invested: {cash.get('invested')}")

    return {
        "account_info": account_info,
        "cash": cash,
    }


def test_portfolio_endpoints(client: T212Client) -> dict[str, Any]:
    """Test portfolio endpoints."""
    print("\n" + "=" * 80)
    print("TESTING PORTFOLIO ENDPOINTS")
    print("=" * 80)

    endpoints = PortfolioEndpoints(client)

    # Test fetch portfolio
    print("\n[1/2] Fetching all open positions...")
    portfolio = endpoints.fetch_portfolio()
    print(f"✓ Found {len(portfolio)} open positions")

    if portfolio:
        # Show first position as sample
        first = portfolio[0]
        print(f"  Sample position: {first.get('ticker')} - Quantity: {first.get('quantity')}")
    else:
        print("  (No open positions in portfolio)")

    # Test fetch pending orders
    print("\n[2/2] Fetching pending orders...")
    orders = endpoints.fetch_orders()
    print(f"✓ Found {len(orders)} pending orders")

    if orders:
        first_order = orders[0]
        print(f"  Sample order: {first_order.get('ticker')} - Type: {first_order.get('type')}")
    else:
        print("  (No pending orders)")

    return {
        "portfolio_count": len(portfolio),
        "pending_orders_count": len(orders),
        "portfolio": portfolio,
        "orders": orders,
    }


def test_metadata_endpoints(client: T212Client) -> dict[str, Any]:
    """Test metadata endpoints."""
    print("\n" + "=" * 80)
    print("TESTING METADATA ENDPOINTS")
    print("=" * 80)

    endpoints = MetadataEndpoints(client)

    # Test exchanges
    print("\n[1/2] Fetching exchanges...")
    exchanges = endpoints.fetch_exchanges()
    print(f"✓ Found {len(exchanges)} exchanges")
    if exchanges:
        print(f"  Sample: {exchanges[0].get('name')} (ID: {exchanges[0].get('id')})")

    # Test instruments
    print("\n[2/2] Fetching instruments (this may take a moment)...")
    instruments = endpoints.fetch_instruments()
    print(f"✓ Found {len(instruments)} tradable instruments")
    if instruments:
        print(f"  Sample: {instruments[0].get('name')} ({instruments[0].get('ticker')})")

    return {
        "exchanges_count": len(exchanges),
        "instruments_count": len(instruments),
        "exchanges": exchanges,
        "instruments": instruments,
    }


def test_history_endpoints_and_pagination(client: T212Client) -> dict[str, Any]:
    """Test historical endpoints with pagination logic."""
    print("\n" + "=" * 80)
    print("TESTING HISTORY ENDPOINTS & PAGINATION")
    print("=" * 80)

    endpoints = HistoryEndpoints(client)
    results = {}

    # Test historical orders with full pagination
    print("\n[1/3] Fetching historical orders (FULL PAGINATION TEST)...")
    print("  Starting with limit=50 per page...")

    orders_response = endpoints.fetch_orders(params={"limit": 50})
    all_orders = orders_response.get("items", [])
    next_page = orders_response.get("nextPagePath")

    print(f"✓ Page 1: {len(all_orders)} orders")

    # Fetch ALL pages to properly test pagination
    page_count = 1

    while next_page:
        # Rate limit: 6 requests per minute for historical orders
        # Wait 10 seconds between pages to stay under limit (6 requests/60s = 1 per 10s)
        print("  Waiting 10 seconds for rate limit...")
        time.sleep(10)

        # Parse the next page path properly
        from urllib.parse import urlparse, parse_qsl

        parsed = urlparse(next_page)

        # Extract path and query parameters
        if parsed.query:
            # Has query string
            params = dict(parse_qsl(parsed.query))
            page_response = endpoints.fetch_orders(params=params)
        elif next_page.startswith("?"):
            # Just query string
            params = dict(parse_qsl(next_page[1:]))
            page_response = endpoints.fetch_orders(params=params)
        else:
            print(f"  Warning: Unexpected nextPagePath format: {next_page}")
            break

        page_items = page_response.get("items", [])
        page_count += 1
        all_orders.extend(page_items)
        next_page = page_response.get("nextPagePath")

        print(f"✓ Page {page_count}: {len(page_items)} orders (Total so far: {len(all_orders)})")

        # Safety limit to avoid infinite loops in testing
        if page_count >= 20:
            print(f"  (Stopping at {page_count} pages for test safety)")
            break

    print(f"\n✓ Pagination complete: {page_count} total pages, {len(all_orders)} total orders fetched")

    if len(all_orders) > 0:
        print(f"  First order date: {all_orders[0].get('dateCreated', 'N/A')}")
        print(f"  Last order date: {all_orders[-1].get('dateCreated', 'N/A')}")

    results["orders"] = {
        "first_page_count": len(orders_response.get("items", [])),
        "pages_fetched": page_count,
        "total_orders": len(all_orders),
        "has_pagination": page_count > 1,
    }

    # Test dividends
    print("\n[2/3] Fetching dividend history...")
    dividends_response = endpoints.fetch_dividends(params={"limit": 50})
    dividends = dividends_response.get("items", [])
    print(f"✓ Found {len(dividends)} dividend records")
    if dividends:
        print(f"  Sample: {dividends[0].get('ticker')} - {dividends[0].get('amountInEuro')}")

    results["dividends"] = {
        "count": len(dividends),
        "has_pagination": bool(dividends_response.get("nextPagePath")),
    }

    # Test transactions
    print("\n[3/3] Fetching transaction history...")
    transactions_response = endpoints.fetch_transactions(params={"limit": 50})
    transactions = transactions_response.get("items", [])
    print(f"✓ Found {len(transactions)} transaction records")
    if transactions:
        print(f"  Sample: {transactions[0].get('type')} - {transactions[0].get('amount')}")

    results["transactions"] = {
        "count": len(transactions),
        "has_pagination": bool(transactions_response.get("nextPagePath")),
    }

    return results


def test_rate_limiting_headers(client: T212Client) -> dict[str, Any]:
    """Test that rate limiting headers are being captured."""
    print("\n" + "=" * 80)
    print("TESTING RATE LIMITING HEADERS")
    print("=" * 80)

    print("\n[1/1] Making API call and checking headers...")
    response = client.get("/equity/account/info", label="/equity/account/info")

    headers = {}
    for key, value in client.iter_rate_limit_headers(response):
        headers[key] = value
        print(f"✓ {key}: {value}")

    if not headers:
        print("  Warning: No rate limit headers found in response")

    return {
        "rate_limit_headers": headers,
    }


def run_all_tests() -> None:
    """Run all endpoint and pagination tests."""
    configure_logging()
    settings = get_settings()

    print("\n" + "=" * 80)
    print("TRADING 212 API - ENDPOINT & PAGINATION TEST SUITE")
    print("=" * 80)
    print(f"Environment: {settings.t212_api_env}")
    print(f"Base URL: {settings.base_url}")
    print("=" * 80)

    all_results = {}

    try:
        with T212Client(settings) as client:
            # Test all endpoints
            all_results["account"] = test_account_endpoints(client)
            all_results["portfolio"] = test_portfolio_endpoints(client)
            all_results["metadata"] = test_metadata_endpoints(client)
            all_results["history"] = test_history_endpoints_and_pagination(client)
            all_results["rate_limiting"] = test_rate_limiting_headers(client)

        # Print summary
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print("✓ All endpoint tests completed successfully!")
        print(f"\nAccount ID: {all_results['account']['account_info'].get('id')}")
        print(f"Open Positions: {all_results['portfolio']['portfolio_count']}")
        print(f"Exchanges: {all_results['metadata']['exchanges_count']}")
        print(f"Instruments: {all_results['metadata']['instruments_count']}")
        print(f"Historical Orders (sampled): {all_results['history']['orders']['total_orders']}")
        print(f"Dividends (sampled): {all_results['history']['dividends']['count']}")
        print(f"Transactions (sampled): {all_results['history']['transactions']['count']}")

        print("\nPagination Status:")
        print(f"  Orders: {'Yes' if all_results['history']['orders']['has_pagination'] else 'No'}")
        print(f"  Dividends: {'Yes' if all_results['history']['dividends']['has_pagination'] else 'No'}")
        print(f"  Transactions: {'Yes' if all_results['history']['transactions']['has_pagination'] else 'No'}")

        print("=" * 80)
        print("✓ ALL TESTS PASSED")
        print("=" * 80)

    except Exception:
        print("\n" + "=" * 80)
        print("✗ TEST FAILED")
        print("=" * 80)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
