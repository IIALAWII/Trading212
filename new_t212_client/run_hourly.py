"""
Incremental hourly data collection for Trading212.

This script collects ALL current data:
- Account cash snapshot (1 API call)
- Portfolio positions (1 API call)  
- Pending orders (1 API call)
- NEW transactions since last collection (incremental, ~1-2 API calls)

Uses smart incremental logic - only fetches NEW data!
Total: ~5 API calls, runs in ~15 seconds.
"""

import logging
from datetime import datetime, timezone

from new_t212_client.services.incremental import IncrementalCollectionService
from new_t212_client.config import get_settings
from new_t212_client.logging_config import configure_logging


def main():
    """Execute incremental hourly data collection."""
    configure_logging()
    logger = logging.getLogger(__name__)

    start_time = datetime.now(timezone.utc)
    logger.info("="*80)
    logger.info("STARTING INCREMENTAL HOURLY COLLECTION")
    logger.info("Started at: %s", start_time.isoformat())
    logger.info("="*80)

    settings = get_settings()

    try:
        service = IncrementalCollectionService(settings)
        summary = service.run()

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        logger.info("="*80)
        logger.info("✓ COLLECTION COMPLETED SUCCESSFULLY")
        logger.info("Duration: %.1f seconds", duration)
        logger.info("Summary: %s", summary)
        logger.info("="*80)

    except Exception as e:
        logger.error("❌ Collection failed: %s", str(e), exc_info=True)
        raise


if __name__ == "__main__":
    main()
