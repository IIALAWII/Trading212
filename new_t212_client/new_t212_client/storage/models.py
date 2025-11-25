"""pydantic models mirroring API payloads."""
from __future__ import annotations

from pydantic import BaseModel


class AccountCash(BaseModel):
    """Placeholder model for `/equity/account/cash`."""

    blocked: float | None = None
    free: float | None = None
    invested: float | None = None
    pieCash: float | None = None
    ppl: float | None = None
    result: float | None = None
    total: float | None = None
