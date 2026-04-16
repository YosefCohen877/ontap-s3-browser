"""
utils/errors.py — ONTAP-specific S3 error classification.

Maps raw boto3/botocore/ssl/socket exceptions to human-readable
diagnostic categories so the UI can surface meaningful messages
instead of raw tracebacks.
"""
from __future__ import annotations

import socket
import ssl
from dataclasses import dataclass
from typing import Optional

import botocore.exceptions


@dataclass
class S3ErrorInfo:
    category: str          # machine-readable category key
    title: str             # short human title
    message: str           # explanation for the user
    detail: Optional[str]  # raw exception string (shown in debug panel)
    http_code: int = 200   # suggested HTTP status for API response


# ── Mapping from exception to S3ErrorInfo ─────────────────────────────────

def classify_exception(exc: Exception) -> S3ErrorInfo:
    """
    Inspect the exception chain and return a structured S3ErrorInfo.
    Order matters: check most-specific first.
    """
    exc_str = str(exc)
    exc_type = type(exc).__name__

    # ── DNS resolution failure ────────────────────────────────────────────
    if isinstance(exc, socket.gaierror) or (
        isinstance(exc, botocore.exceptions.EndpointResolutionError)
    ):
        return S3ErrorInfo(
            category="dns_failure",
            title="DNS Resolution Failed",
            message=(
                "The S3 endpoint hostname could not be resolved. "
                "Check S3_ENDPOINT_URL and ensure the hostname is reachable "
                "from inside the container (add extra_hosts if needed)."
            ),
            detail=exc_str,
            http_code=502,
        )

    # ── TCP connection refused / timeout ─────────────────────────────────
    if isinstance(exc, (ConnectionRefusedError, socket.timeout)):
        return S3ErrorInfo(
            category="tcp_failure",
            title="TCP Connection Failed",
            message=(
                "The endpoint hostname resolved but the TCP connection was refused "
                "or timed out. Check that the S3 service is running and the port is correct."
            ),
            detail=exc_str,
            http_code=502,
        )

    # ── TLS handshake / version negotiation (ONTAP-specific) ─────────────
    if isinstance(exc, ssl.SSLError):
        if "WRONG_VERSION_NUMBER" in exc_str or "handshake" in exc_str.lower():
            return S3ErrorInfo(
                category="tls_handshake_failure",
                title="TLS Handshake Failed",
                message=(
                    "The TLS handshake failed. ONTAP S3 may not support the TLS version "
                    "or cipher suites offered by this client. "
                    "Ensure the endpoint supports TLS 1.2, or check ONTAP TLS settings."
                ),
                detail=exc_str,
                http_code=502,
            )
        if "CERTIFICATE_VERIFY_FAILED" in exc_str or isinstance(exc, ssl.SSLCertVerificationError):
            return S3ErrorInfo(
                category="cert_trust_failure",
                title="Certificate Trust Failure",
                message=(
                    "The server's TLS certificate could not be verified. "
                    "Set S3_CA_BUNDLE to your internal root CA bundle path, "
                    "or set S3_VERIFY_SSL=false temporarily for testing."
                ),
                detail=exc_str,
                http_code=502,
            )
        return S3ErrorInfo(
            category="ssl_error",
            title="SSL/TLS Error",
            message=f"An SSL error occurred: {exc_str}",
            detail=exc_str,
            http_code=502,
        )

    # ── botocore-wrapped connection errors ────────────────────────────────
    if isinstance(exc, botocore.exceptions.ConnectTimeoutError):
        return S3ErrorInfo(
            category="connect_timeout",
            title="Connection Timeout",
            message="Connection to the S3 endpoint timed out. Check network reachability and S3_CONNECT_TIMEOUT.",
            detail=exc_str,
            http_code=504,
        )

    if isinstance(exc, botocore.exceptions.ReadTimeoutError):
        return S3ErrorInfo(
            category="read_timeout",
            title="Read Timeout",
            message="The S3 endpoint did not respond in time. Check S3_READ_TIMEOUT or server load.",
            detail=exc_str,
            http_code=504,
        )

    # ── boto3 ClientError (S3 API errors) ────────────────────────────────
    if isinstance(exc, botocore.exceptions.ClientError):
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", exc_str)

        if code in ("InvalidAccessKeyId", "InvalidAccessKey"):
            return S3ErrorInfo(
                category="auth_failure_key",
                title="Invalid Access Key",
                message="The access key ID is not recognised by ONTAP S3. Check S3_ACCESS_KEY_ID.",
                detail=f"[{code}] {msg}",
                http_code=403,
            )
        if code == "SignatureDoesNotMatch":
            return S3ErrorInfo(
                category="auth_failure_signature",
                title="Signature Mismatch",
                message=(
                    "Request signature does not match. Causes: wrong secret key, "
                    "wrong region, or virtual-style addressing when path-style is required. "
                    "Ensure S3_ADDRESSING_STYLE=path and S3_REGION matches ONTAP config."
                ),
                detail=f"[{code}] {msg}",
                http_code=403,
            )
        if code == "NoSuchBucket":
            return S3ErrorInfo(
                category="no_such_bucket",
                title="Bucket Not Found",
                message=f"Bucket does not exist on this ONTAP S3 server: {msg}",
                detail=f"[{code}] {msg}",
                http_code=404,
            )
        if code == "NoSuchKey":
            return S3ErrorInfo(
                category="no_such_key",
                title="Object Not Found",
                message=f"Object key does not exist: {msg}",
                detail=f"[{code}] {msg}",
                http_code=404,
            )
        if code == "AccessDenied":
            return S3ErrorInfo(
                category="access_denied",
                title="Access Denied",
                message="The S3 credentials do not have permission for this operation.",
                detail=f"[{code}] {msg}",
                http_code=403,
            )
        # Generic S3 API error
        return S3ErrorInfo(
            category="s3_api_error",
            title=f"S3 API Error ({code})",
            message=msg,
            detail=f"[{code}] {msg}",
            http_code=500,
        )

    # ── botocore NoCredentialsError ───────────────────────────────────────
    if isinstance(exc, botocore.exceptions.NoCredentialsError):
        return S3ErrorInfo(
            category="no_credentials",
            title="No Credentials",
            message="S3_ACCESS_KEY_ID or S3_SECRET_ACCESS_KEY is not configured.",
            detail=exc_str,
            http_code=500,
        )

    # ── Fallback ──────────────────────────────────────────────────────────
    return S3ErrorInfo(
        category="unknown_error",
        title=f"Unexpected Error ({exc_type})",
        message=exc_str,
        detail=exc_str,
        http_code=500,
    )
