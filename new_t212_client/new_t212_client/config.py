"""Configuration helpers for the T212 client."""
from __future__ import annotations

import base64
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Typed settings sourced from environment variables."""

    t212_api_key: str = Field(..., env="T212_API_KEY")
    t212_api_secret: str = Field(..., env="T212_API_SECRET")
    t212_api_env: Literal["live", "demo"] = Field("demo", env="T212_API_ENV")

    sqlserver_server: str = Field("localhost", env="SQLSERVER_SERVER")
    sqlserver_database: str = Field(..., env="SQLSERVER_DATABASE")
    sqlserver_username: str | None = Field(None, env="SQLSERVER_USERNAME")
    sqlserver_password: str | None = Field(None, env="SQLSERVER_PASSWORD")
    sqlserver_driver: str = Field(
        "ODBC Driver 18 for SQL Server", env="SQLSERVER_DRIVER"
    )
    sqlserver_encrypt: Literal["mandatory", "optional"] = Field(
        "mandatory", env="SQLSERVER_ENCRYPT"
    )
    sqlserver_trust_server_certificate: bool = Field(
        False, env="SQLSERVER_TRUST_SERVER_CERTIFICATE"
    )

    class Config:
        """Pydantic configuration."""

        env_file = Path(__file__).resolve().parent.parent.parent / "new_t212_client" / ".env"
        env_file_encoding = "utf-8"

    @property
    def base_url(self) -> str:
        """Get API base URL based on environment."""
        if self.t212_api_env == "live":
            return "https://live.trading212.com/api/v0"
        return "https://demo.trading212.com/api/v0"

    @property
    def auth_header(self) -> str:
        """Get Basic auth header."""
        return f"Basic {self._encoded_credentials}"  # pragma: no cover simple property

    @property
    def sqlserver_dsn_kwargs(self) -> dict[str, str | bool]:
        """Get SQL Server connection parameters."""
        return {
            "driver": self.sqlserver_driver,
            "server": self.sqlserver_server,
            "database": self.sqlserver_database,
            "trusted_connection": "yes" if not self.sqlserver_username else "no",
            "encrypt": self.sqlserver_encrypt,
            "trust_server_certificate": "yes" if self.sqlserver_trust_cert else "no",
        }

    @property
    def _encoded_credentials(self) -> str:
        """Encode API credentials for Basic auth."""
        raw = f"{self.t212_api_key}:{self.t212_api_secret}".encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    @property
    def sqlserver_trust_cert(self) -> bool:
        """Get trust server certificate setting."""
        return self.sqlserver_trust_server_certificate


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""

    return Settings()  # type: ignore[arg-type]
