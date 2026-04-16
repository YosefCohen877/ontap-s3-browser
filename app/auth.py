"""
auth.py — Simple HTTP Basic authentication dependency for FastAPI.

Credentials are read from env vars (WEB_USERNAME / WEB_PASSWORD).
No secrets are ever exposed to the frontend.
"""
from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.config import get_settings

security = HTTPBasic()


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """FastAPI dependency — inject into any route that needs auth."""
    cfg = get_settings()

    correct_username = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        cfg.web_username.encode("utf-8"),
    )
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        cfg.web_password.encode("utf-8"),
    )

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": 'Basic realm="ONTAP S3 Browser"'},
        )

    return credentials.username


def enforce_bucket_access(requested_bucket: str) -> None:
    """
    Ensure the user is accessing the correct bucket when s3_forced_bucket is set.
    Raises 403 Forbidden if the bucket name doesn't match.
    """
    cfg = get_settings()
    if cfg.s3_forced_bucket and requested_bucket != cfg.s3_forced_bucket:
        from app.utils.logging import get_logger
        logger = get_logger(__name__)
        logger.warning(
            "auth.bucket_access_denied",
            requested=requested_bucket,
            forced=cfg.s3_forced_bucket
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "category": "access_denied",
                "title": "Access Denied",
                "message": f"This instance is locked to bucket: {cfg.s3_forced_bucket}",
                "detail": f"You tried to access: {requested_bucket}",
            }
        )
