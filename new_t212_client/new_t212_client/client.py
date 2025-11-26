"""HTTP client wrapper for Trading 212."""
from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Iterable, Mapping

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth import build_auth_headers
from .config import Settings, get_settings
from .rate_limiter import rate_limiter as shared_rate_limiter

DEFAULT_TIMEOUT = httpx.Timeout(30.0)


class T212Client(AbstractContextManager["T212Client"]):
    """Simple synchronous Trading 212 API client."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client = httpx.Client(base_url=self.settings.base_url, timeout=DEFAULT_TIMEOUT)
        self._rate_limiter = shared_rate_limiter

    def __enter__(self) -> "T212Client":  # pragma: no cover trivial
        return self

    def __exit__(self, *exc_info: object) -> None:  # pragma: no cover trivial
        self.close()

    def close(self) -> None:
        '''Close underlying HTTP client.'''  # pragma: no cover trivial
        self._client.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.HTTPStatusError)),
    )
    def get(self, path: str, params: Mapping[str, Any] | None = None, *,
            label: str | None = None) -> httpx.Response:
        """Issue a GET request with shared headers and retry strategy."""

        rate_limit_key = label or path
        self._rate_limiter.wait(rate_limit_key)

        headers = build_auth_headers(self.settings)
        target = self._normalise_path(path)
        response = self._client.get(target, params=params, headers=headers)
        response.raise_for_status()

        header_dict = {key: value for key, value in self.iter_rate_limit_headers(response)}
        if header_dict:
            self._rate_limiter.update_from_headers(rate_limit_key, header_dict)
        return response

    def _normalise_path(self, path: str) -> str:
        """Convert form-agnostic endpoint paths into httpx-friendly targets."""

        if path.startswith(("http://", "https://")):
            return path
        cleaned = path.lstrip("/")
        if cleaned.startswith("api/v0/"):
            cleaned = cleaned[len("api/v0/"):]
        return cleaned

    def iter_rate_limit_headers(self, response: httpx.Response) -> Iterable[tuple[str, str]]:
        """Yield rate-limit metadata which can be passed to the rate limiter."""

        for key in (
            "x-ratelimit-limit",
            "x-ratelimit-period",
            "x-ratelimit-remaining",
            "x-ratelimit-reset",
            "x-ratelimit-used",
        ):
            if key in response.headers:
                yield key, response.headers[key]
