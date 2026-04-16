"""
routers/diagnostics.py — Health check and ONTAP connection test endpoints.

/api/health  — quick liveness check (no S3 call)
/api/test-connection — step-by-step probe:
  Step 1: DNS resolution
  Step 2: TCP connect
  Step 3: TLS handshake + certificate inspection
  Step 4: S3 API call (list_buckets)
"""
from __future__ import annotations

import socket
import ssl
import time
import urllib.parse
from typing import Any

from fastapi import APIRouter, Depends

from app.auth import require_auth
from app.config import get_settings
from app.s3_client import create_s3_client
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["diagnostics"])
logger = get_logger(__name__)


def _step_result(
    step: int,
    name: str,
    status: str,          # "ok" | "failed" | "skipped"
    message: str,
    detail: Any = None,
    duration_ms: float = 0,
) -> dict:
    return {
        "step": step,
        "name": name,
        "status": status,
        "message": message,
        "detail": detail,
        "duration_ms": round(duration_ms, 1),
    }


@router.get("/health")
def health():
    """Simple liveness probe — no S3 call, exposes feature flags."""
    cfg = get_settings()
    return {
        "status": "ok",
        "service": "ontap-s3-browser",
        "features": {
            "upload": cfg.enable_upload,
            "delete": cfg.enable_delete,
            "create_bucket": cfg.enable_create_bucket,
            "bucket_count": cfg.enable_bucket_count,
        }
    }


@router.get("/test-connection")
def test_connection(username: str = Depends(require_auth)):
    """
    Run four sequential probes against the configured ONTAP S3 endpoint.
    Returns structured JSON with per-step status and detailed diagnostics.
    """
    cfg = get_settings()
    results = []
    overall = "ok"

    parsed = urllib.parse.urlparse(cfg.s3_endpoint_url)
    hostname = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    use_tls = parsed.scheme == "https"

    logger.info("test_connection.start", endpoint=cfg.s3_endpoint_url, host=hostname, port=port)

    # ── Step 1: DNS ───────────────────────────────────────────────────────
    t0 = time.monotonic()
    resolved_ip = None
    try:
        resolved_ip = socket.gethostbyname(hostname)
        duration = (time.monotonic() - t0) * 1000
        results.append(_step_result(
            1, "DNS Resolution", "ok",
            f"Resolved '{hostname}' → {resolved_ip}",
            detail={"hostname": hostname, "ip": resolved_ip},
            duration_ms=duration,
        ))
        logger.info("test_connection.dns.ok", hostname=hostname, ip=resolved_ip)
    except socket.gaierror as exc:
        duration = (time.monotonic() - t0) * 1000
        results.append(_step_result(
            1, "DNS Resolution", "failed",
            f"Cannot resolve hostname '{hostname}': {exc}",
            detail=str(exc),
            duration_ms=duration,
        ))
        logger.error("test_connection.dns.failed", hostname=hostname, error=str(exc))
        overall = "failed"
        return {"overall": overall, "endpoint": cfg.s3_endpoint_url, "steps": results}

    # ── Step 2: TCP connect ───────────────────────────────────────────────
    t0 = time.monotonic()
    try:
        sock = socket.create_connection((hostname, port), timeout=cfg.s3_connect_timeout)
        sock.close()
        duration = (time.monotonic() - t0) * 1000
        results.append(_step_result(
            2, "TCP Connect", "ok",
            f"TCP connection to {hostname}:{port} succeeded",
            detail={"host": hostname, "port": port},
            duration_ms=duration,
        ))
        logger.info("test_connection.tcp.ok", host=hostname, port=port)
    except (ConnectionRefusedError, socket.timeout, OSError) as exc:
        duration = (time.monotonic() - t0) * 1000
        results.append(_step_result(
            2, "TCP Connect", "failed",
            f"TCP connection to {hostname}:{port} failed: {exc}",
            detail=str(exc),
            duration_ms=duration,
        ))
        logger.error("test_connection.tcp.failed", host=hostname, port=port, error=str(exc))
        overall = "failed"
        return {"overall": overall, "endpoint": cfg.s3_endpoint_url, "steps": results}

    # ── Step 3: TLS handshake (HTTPS only) ───────────────────────────────
    if use_tls:
        t0 = time.monotonic()
        try:
            ctx = ssl.create_default_context()
            if cfg.s3_ca_bundle:
                ctx.load_verify_locations(cafile=cfg.s3_ca_bundle)
            if not cfg.s3_verify_ssl:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

            raw = socket.create_connection((hostname, port), timeout=cfg.s3_connect_timeout)
            tls_sock = ctx.wrap_socket(raw, server_hostname=hostname)
            cert = tls_sock.getpeercert()
            tls_version = tls_sock.version()
            cipher = tls_sock.cipher()
            tls_sock.close()

            duration = (time.monotonic() - t0) * 1000
            subject = dict(x[0] for x in cert.get("subject", []))
            issuer = dict(x[0] for x in cert.get("issuer", []))
            results.append(_step_result(
                3, "TLS Handshake", "ok",
                f"TLS {tls_version} handshake succeeded. Cipher: {cipher[0]}",
                detail={
                    "tls_version": tls_version,
                    "cipher": cipher[0],
                    "cert_subject": subject,
                    "cert_issuer": issuer,
                    "cert_expires": cert.get("notAfter"),
                },
                duration_ms=duration,
            ))
            logger.info("test_connection.tls.ok", version=tls_version, cipher=cipher[0])

        except ssl.SSLCertVerificationError as exc:
            duration = (time.monotonic() - t0) * 1000
            if not cfg.s3_verify_ssl:
                results.append(_step_result(
                    3, "TLS Handshake", "skipped",
                    f"Certificate invalid but S3_VERIFY_SSL=false — verification skipped. Issue: {exc}",
                    detail=str(exc),
                    duration_ms=duration,
                ))
                logger.warning("test_connection.tls.cert_skipped", error=str(exc))
            else:
                results.append(_step_result(
                    3, "TLS Handshake", "failed",
                    f"Certificate verification failed: {exc}. Set S3_CA_BUNDLE to your internal CA.",
                    detail=str(exc),
                    duration_ms=duration,
                ))
                logger.error("test_connection.tls.cert_error", error=str(exc))
                overall = "failed"
                return {"overall": overall, "endpoint": cfg.s3_endpoint_url, "steps": results}

        except ssl.SSLError as exc:
            duration = (time.monotonic() - t0) * 1000
            if not cfg.s3_verify_ssl:
                results.append(_step_result(
                    3, "TLS Handshake", "skipped",
                    f"TLS error but S3_VERIFY_SSL=false — verification skipped. Issue: {exc}",
                    detail=str(exc),
                    duration_ms=duration,
                ))
                logger.warning("test_connection.tls.ssl_skipped", error=str(exc))
            else:
                hint = (
                    "ONTAP TLS negotiation failed — check that ONTAP allows TLS 1.2 and "
                    "the ciphers supported by this Python version."
                    if "WRONG_VERSION_NUMBER" in str(exc) or "handshake" in str(exc).lower()
                    else str(exc)
                )
                results.append(_step_result(
                    3, "TLS Handshake", "failed",
                    f"TLS error: {hint}",
                    detail=str(exc),
                    duration_ms=duration,
                ))
                logger.error("test_connection.tls.ssl_error", error=str(exc))
                overall = "failed"
                return {"overall": overall, "endpoint": cfg.s3_endpoint_url, "steps": results}

    else:
        results.append(_step_result(
            3, "TLS Handshake", "skipped",
            "Endpoint uses plain HTTP — TLS check skipped.",
        ))

    # ── Step 4: S3 API call ───────────────────────────────────────────────
    t0 = time.monotonic()
    try:
        client = create_s3_client(cfg)
        
        if cfg.s3_forced_bucket:
            # In forced mode, check access to the specific bucket instead of listing all
            client.head_bucket(Bucket=cfg.s3_forced_bucket)
            duration = (time.monotonic() - t0) * 1000
            results.append(_step_result(
                4, "S3 API (head_bucket)", "ok",
                f"Successfully verified access to forced bucket: {cfg.s3_forced_bucket}",
                detail={"forced_bucket": cfg.s3_forced_bucket},
                duration_ms=duration,
            ))
            logger.info("test_connection.s3_api.forced_ok", bucket=cfg.s3_forced_bucket)
        else:
            # Standard mode: list all buckets
            resp = client.list_buckets()
            bucket_count = len(resp.get("Buckets", []))
            duration = (time.monotonic() - t0) * 1000
            results.append(_step_result(
                4, "S3 API (list_buckets)", "ok",
                f"ONTAP S3 responded successfully. Found {bucket_count} bucket(s).",
                detail={"bucket_count": bucket_count},
                duration_ms=duration,
            ))
            logger.info("test_connection.s3_api.ok", bucket_count=bucket_count)

    except Exception as exc:
        from app.utils.errors import classify_exception
        info = classify_exception(exc)
        duration = (time.monotonic() - t0) * 1000
        results.append(_step_result(
            4, "S3 API (list_buckets)", "failed",
            f"{info.title}: {info.message}",
            detail=info.detail,
            duration_ms=duration,
        ))
        logger.error("test_connection.s3_api.failed", category=info.category, detail=info.detail)
        overall = "failed"

    return {
        "overall": overall,
        "endpoint": cfg.s3_endpoint_url,
        "region": cfg.s3_region,
        "addressing_style": cfg.s3_addressing_style,
        "tls_verify": cfg.s3_verify_ssl,
        "ca_bundle": cfg.s3_ca_bundle,
        "steps": results,
    }
