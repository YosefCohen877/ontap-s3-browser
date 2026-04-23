"""
config.py — Centralised configuration via pydantic-settings.
All settings are read from environment variables or a .env file.
No hardcoded AWS/ONTAP assumptions live here; they belong in s3_client.py.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── S3 / ONTAP credentials ──────────────────────────────────────────────
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str = "us-east-1"
    s3_endpoint_url: str  # e.g. https://trident-k8s-s3 or https://mini:9000/bucket
    s3_forced_bucket: Optional[str] = None  # Extracted internally from endpoint path

    # ── TLS / CA options ────────────────────────────────────────────────────
    s3_ca_bundle: Optional[str] = None   # path to CA bundle PEM inside container
    s3_verify_ssl: bool = True           # set False ONLY for local testing

    # ── S3 client tuning (ONTAP-specific) ───────────────────────────────────
    s3_addressing_style: str = "path"     # ONTAP requires path-style
    s3_max_pool_connections: int = 5      # Conservative — avoid TLS overload on ONTAP
    s3_connect_timeout: int = 10
    s3_read_timeout: int = 30

    # ── App server ──────────────────────────────────────────────────────────
    app_port: int = 8080

    log_level: str = "INFO"

    # ── Object preview limits (bytes) ───────────────────────────────────────
    # Text/JSON/log previews read at most this many bytes (memory-safe).
    preview_max_text_bytes: int = Field(default=512 * 1024, ge=1024, le=100 * 1024 * 1024)
    # Images, PDFs, and video stream previews refuse objects larger than this.
    preview_max_binary_bytes: int = Field(default=20 * 1024 * 1024, ge=64 * 1024, le=2 * 1024**3)

    # ── Web UI authentication ────────────────────────────────────────────────
    web_username: str = "admin"
    web_password: str  # required — no default, must be set in env

    # ── Feature flags ───────────────────────────────────────────────────────
    enable_upload: bool = False
    enable_delete: bool = False
    enable_create_bucket: bool = False
    enable_delete_bucket: bool = False
    enable_bucket_count: bool = True
    enable_bucket_lifecycle: bool = False
    # Replace/merge S3 object tags (PutObjectTagging / GetObjectTagging)
    enable_object_tagging: bool = True

    @model_validator(mode="before")
    @classmethod
    def parse_endpoint_path(cls, data: dict) -> dict:
        """
        Extract bucket name from S3_ENDPOINT_URL if present.
        Example: http://mini:9000/testbucket -> endpoint=http://mini:9000, forced=testbucket
        """
        import os
        from urllib.parse import urlparse

        # Pydantic-settings handles env names; we check the dict keys which are usually lowercase
        url = data.get("s3_endpoint_url")
        if not url:
            # Check environment directly if not in data (fallback)
            url = os.environ.get("S3_ENDPOINT_URL")

        if url:
            parsed = urlparse(url)
            # If path exists and isn't just '/', the first segment is the bucket
            if parsed.path and parsed.path != "/":
                path_parts = parsed.path.strip("/").split("/")
                if path_parts:
                    data["s3_forced_bucket"] = path_parts[0]
                    # Rewrite endpoint to be just the scheme + netloc (host:port)
                    # This ensures boto3 doesn't try to use the path in the base connection
                    data["s3_endpoint_url"] = f"{parsed.scheme}://{parsed.netloc}"

        return data

    @field_validator("s3_endpoint_url")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @field_validator("s3_addressing_style")
    @classmethod
    def validate_addressing_style(cls, v: str) -> str:
        v = v.lower()
        if v not in {"path", "virtual", "auto"}:
            raise ValueError("s3_addressing_style must be path | virtual | auto")
        return v

    @model_validator(mode="after")
    def warn_ssl_disabled(self) -> "Settings":
        if not self.s3_verify_ssl:
            import warnings
            warnings.warn(
                "S3_VERIFY_SSL=false — TLS certificate verification is DISABLED. "
                "Use only for debugging in isolated environments.",
                stacklevel=2,
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
