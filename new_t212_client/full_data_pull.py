"""Full data pull script - fetches ALL data from ALL endpoints and saves to SQL Server."""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from new_t212_client.config import get_settings
from new_t212_client.logging_config import configure_logging
from new_t212_client.services.ingestion import IngestionService

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))


def main() -> None:
    """Run a complete data pull from all Trading 212 endpoints.""" 
    # Configure logging to logs directory
    log_dir = Path(__file__).parent / "logs"
    configure_logging(log_dir=log_dir)

    settings = get_settings()

    start_time = datetime.utcnow()
    logging.info("=" * 80)
    logging.info("STARTING FULL DATA PULL")
    logging.info("=" * 80)
    logging.info("Environment: %s", settings.t212_api_env)
    logging.info("Database: %s", settings.sqlserver_database)
    logging.info("Started at: %s UTC", start_time.isoformat())
    logging.info("=" * 80)

    try:
        # Run the full ingestion
        service = IngestionService(settings=settings)
        summary = service.run_full_snapshot()

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        logging.info("=" * 80)
        logging.info("FULL DATA PULL COMPLETED SUCCESSFULLY")
        logging.info("=" * 80)
        logging.info("Duration: %.2f seconds", duration)
        logging.info("Summary:")
        for key, value in summary.items():
            logging.info("  %s: %s", key, value)
        logging.info("=" * 80)

        print("\n✓ Full data pull completed successfully!")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Logs saved to: {log_dir / 't212_ingestion.log'}")

    except Exception:
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        logging.error("=" * 80)
        logging.error("FULL DATA PULL FAILED")
        logging.error("=" * 80)
        logging.error("Duration before failure: %.2f seconds", duration)
        logging.error("=" * 80)
        logging.exception("Full traceback:")

        print(f"\n✗ Full data pull failed after {duration:.2f} seconds")
        print(f"  Logs saved to: {log_dir / 't212_ingestion.log'}")
        sys.exit(1)


if __name__ == "__main__":
    main()
