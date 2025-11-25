"""Rate limiter aware of Trading 212 response headers."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)


@dataclass
class RateLimitState:
    """Trading212 API rate limit state."""

    limit: int
    period: int
    remaining: int
    reset_epoch: int


class RateLimiter:
    """Simple cooperative rate limiter."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, RateLimitState] = {}
        self._first_calls: set[str] = set()  # Track first calls to avoid unnecessary waits

    def update_from_headers(self, endpoint: str, headers: dict[str, str]) -> None:
        """Refresh in-memory state after each response."""

        limit = headers.get("x-ratelimit-limit")
        period = headers.get("x-ratelimit-period")
        remaining = headers.get("x-ratelimit-remaining")
        reset_epoch = headers.get("x-ratelimit-reset")
        if not all((limit, period, remaining, reset_epoch)):
            return
        with self._lock:
            self._state[endpoint] = RateLimitState(
                limit=int(limit),
                period=int(period),
                remaining=int(remaining),
                reset_epoch=int(reset_epoch),
            )

    def wait(self, endpoint: str) -> None:
        """Sleep if the recorded state suggests the bucket is empty."""

        with self._lock:
            state = self._state.get(endpoint)
            is_first_call = endpoint not in self._first_calls
            self._first_calls.add(endpoint)

        if not state:
            # No rate limit info yet - only wait for history endpoints on subsequent calls
            if 'history' in endpoint and not is_first_call:
                LOGGER.info(
                    "  ⏸ No rate limit info for %s - waiting 10 seconds (conservative)",
                    endpoint,
                )
                time.sleep(10)
            return

        # If we have remaining requests, check if we need to pace ourselves
        if state.remaining > 0:
            # For history endpoints with 6 req/min limit, wait 10 seconds between calls
            if 'history' in endpoint and state.limit == 6:
                LOGGER.info("  ⏸ Pacing requests for %s - waiting 10 seconds", endpoint)
                time.sleep(10)
            return

        # Bucket is empty, wait until reset
        wait_seconds = max(0, state.reset_epoch - int(time.time()))
        if wait_seconds > 0:
            LOGGER.info(
                "  ⏸ Rate limit reached for %s - waiting %d seconds...",
                endpoint,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            LOGGER.info("  ▶ Resuming requests to %s", endpoint)


rate_limiter = RateLimiter()
