"""Smoke test for the new Trading 212 ingestion client."""
from __future__ import annotations

import argparse
import logging
import sys
from importlib import import_module
from pathlib import Path
from pprint import pprint

from sqlalchemy import text
from sqlalchemy.exc import InterfaceError, OperationalError

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PACKAGE_ROOT.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    T212Client = import_module("new_t212_client.client").T212Client
    get_settings = import_module("new_t212_client.config").get_settings
    AccountEndpoints = import_module("new_t212_client.endpoints.account").AccountEndpoints
    configure_logging = import_module("new_t212_client.logging_config").configure_logging
    IngestionService = import_module("new_t212_client.services.ingestion").IngestionService
    create_sql_engine = import_module("new_t212_client.storage.sql_server").create_sql_engine
except ModuleNotFoundError:
    repo_root = PROJECT_ROOT.parent
    sys.path.insert(0, str(repo_root))
    T212Client = import_module("new_t212_client.client").T212Client
    get_settings = import_module("new_t212_client.config").get_settings
    AccountEndpoints = import_module("new_t212_client.endpoints.account").AccountEndpoints
    configure_logging = import_module("new_t212_client.logging_config").configure_logging
    IngestionService = import_module("new_t212_client.services.ingestion").IngestionService
    create_sql_engine = import_module("new_t212_client.storage.sql_server").create_sql_engine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trading212 client smoke-test")
    parser.add_argument(
        "--full-run",
        action="store_true",
        help="Execute the full SQL ingestion after performing connectivity checks.",
    )
    return parser.parse_args()


def check_api(settings) -> dict[str, object]:
    with T212Client(settings) as client:
        account_endpoints = AccountEndpoints(client)
        account_info = account_endpoints.fetch_info()
        cash_snapshot = account_endpoints.fetch_cash()
    return {
        "account_info": account_info,
        "cash_snapshot": cash_snapshot,
    }


def check_database(settings) -> bool:
    engine = create_sql_engine(settings)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            value = result.scalar_one()
        return value == 1
    except (InterfaceError, OperationalError) as exc:
        available = "unknown"
        try:
            import pyodbc  # type: ignore[import-not-found]

            drivers = pyodbc.drivers()
            available = ", ".join(drivers) if drivers else "none"
        except Exception:  # pragma: no cover - diagnostic path
            available = "unavailable"

        message = (
            "Unable to connect to SQL Server via pyodbc. "
            f"Ensure the ODBC driver '{settings.sqlserver_driver}' is installed "
            "and that the SQL Server instance is reachable. "
            "If you are running a local developer instance try setting "
            "SQLSERVER_SERVER to 'localhost\\SQLEXPRESS' (or your instance name) "
            "and confirm the service is started. "
            "If you are using a self-signed certificate set "
            "SQLSERVER_TRUST_SERVER_CERTIFICATE=true in your .env. "
            f"Detected drivers: {available}."
        )
        raise RuntimeError(message) from exc
    finally:
        engine.dispose()


def main() -> None:
    configure_logging()
    args = parse_args()
    settings = get_settings()

    logging.info("Using Trading212 environment: %s", settings.t212_api_env)

    api_data = check_api(settings)
    logging.info("Fetched account info successfully")
    pprint({"account_info": api_data["account_info"]})

    db_ok = check_database(settings)
    logging.info("Database connectivity check passed: %s", db_ok)

    if args.full_run:
        service = IngestionService(settings=settings)
        summary = service.run_full_snapshot()
        pprint({"ingestion_summary": summary})
    else:
        logging.info("Skipping full ingestion; run with --full-run to persist snapshots.")


if __name__ == "__main__":  # pragma: no cover - manual script
    main()
