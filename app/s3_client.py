"""
s3_client.py — Centralised boto3 S3 client factory for NetApp ONTAP S3.

All ONTAP compatibility settings live here:
  - Signature version v4 (required by ONTAP)
  - Path-style addressing (required by ONTAP — virtual-style breaks)
  - Custom endpoint URL (internal DNS, private IP)
  - Custom CA bundle or TLS verification disabled
  - Conservative connection pool to avoid TLS handshake overload
  - Retry logic for transient failures

No other module should create an S3 client directly.
"""
from __future__ import annotations

import ssl
import threading
import urllib3

import boto3
import botocore.config

from app.config import Settings, get_settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

# Hide unverified HTTPS request warnings when TLS verification is intentionally disabled.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# One client instance per process — protected by a lock during creation
_client_lock = threading.Lock()
_s3_client = None


def _resolve_verify_param(cfg: Settings):
    """
    Return the `verify` parameter for boto3.client().

    Priority:
      1. s3_ca_bundle path  → use that PEM file
      2. s3_verify_ssl=False → disable verification (logs a warning)
      3. default             → True (OS / container trust store)
    
    NOTE: We cannot pass ssl.SSLContext objects directly to boto3.
    Instead, we monkey-patch the connection layer below.
    """
    if cfg.s3_ca_bundle:
        logger.info("s3_client.tls", mode="ca_bundle", path=cfg.s3_ca_bundle)
        return cfg.s3_ca_bundle

    if not cfg.s3_verify_ssl:
        logger.warning(
            "s3_client.tls",
            mode="verify_disabled_with_legacy_support",
            message="TLS certificate verification is DISABLED with legacy protocol support — use only for testing",
        )
        # Return False (not SSLContext) to avoid boto3/urllib3 errors
        # The actual SSL context will be applied via monkey-patching below
        return False

    logger.info("s3_client.tls", mode="os_trust_store")
    return True


def create_s3_client(cfg: Settings | None = None):
    """
    Create and return a fresh boto3 S3 client configured for ONTAP S3.
    Prefer `get_s3_client()` for the shared singleton.
    """
    if cfg is None:
        cfg = get_settings()

    verify = _resolve_verify_param(cfg)

    boto_config = botocore.config.Config(
        # ── ONTAP compatibility ──────────────────────────────────────────
        signature_version="s3v4",          # ONTAP requires SigV4
        s3={"addressing_style": cfg.s3_addressing_style},  # path-style for ONTAP

        # ── Connection tuning ────────────────────────────────────────────
        # Keep pool small — ONTAP has finite TLS handshake capacity
        max_pool_connections=cfg.s3_max_pool_connections,
        connect_timeout=cfg.s3_connect_timeout,
        read_timeout=cfg.s3_read_timeout,

        # ── Retry logic ──────────────────────────────────────────────────
        retries={
            "max_attempts": 3,
            "mode": "standard",  # exponential back-off with jitter
        },
    )

    logger.info(
        "s3_client.create",
        endpoint=cfg.s3_endpoint_url,
        region=cfg.s3_region,
        addressing_style=cfg.s3_addressing_style,
        pool_size=cfg.s3_max_pool_connections,
        verify=str(verify),
    )

    client = boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint_url,
        aws_access_key_id=cfg.s3_access_key_id,
        aws_secret_access_key=cfg.s3_secret_access_key,
        region_name=cfg.s3_region,
        verify=verify,
        config=boto_config,
    )
    return client


def get_s3_client():
    """
    Return the shared singleton S3 client.
    Thread-safe; creates client on first call.
    """
    global _s3_client
    if _s3_client is None:
        with _client_lock:
            if _s3_client is None:  # double-checked locking
                _s3_client = create_s3_client()
    return _s3_client




# ── Monkey-patch urllib3 to allow SSLv3 and skip hostname checking ──────────
def _patch_urllib3_for_insecure_https():
    import ssl
    import urllib3.connection

    _original_connect = urllib3.connection.HTTPSConnection.connect

    def patched_connect(self):
        # Build a permissive SSLContext before calling the original connect
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
        except ssl.SSLError:
            pass
        # Inject the context so urllib3 uses it instead of building its own
        self.ssl_context = ctx
        _original_connect(self)

    urllib3.connection.HTTPSConnection.connect = patched_connect
    logger.info("s3_client.urllib3_patched", message="HTTPSConnection.connect patched for legacy ONTAP TLS")
# Apply patch on module load if verify_ssl is False
try:
    cfg = get_settings()
    if not cfg.s3_verify_ssl:
        _patch_urllib3_for_insecure_https()
except Exception as e:
    logger.warning("s3_client.patch_config_load_failed", error=str(e))
