"""
routers/buckets.py — List S3 buckets.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth import enforce_bucket_access, require_auth
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["buckets"])
logger = get_logger(__name__)


@router.get("/buckets")
def list_buckets(username: str = Depends(require_auth)):
    """Return all buckets accessible with the configured credentials."""
    try:
        cfg = get_settings()

        # ── Forced single bucket mode ─────────────────────────────────────────
        # If the endpoint URL included a path (e.g. /mybucket), we only show that.
        if cfg.s3_forced_bucket:
            logger.info("buckets.list.forced", bucket=cfg.s3_forced_bucket, user=username)
            return {
                "buckets": [{"name": cfg.s3_forced_bucket, "created": None}],
                "forced": True
            }

        client = get_s3_client()

        # Get buckets from S3 API if possible
        buckets = []
        try:
            response = client.list_buckets()
            for b in response.get("Buckets", []):
                buckets.append({
                    "name": b["Name"],
                    "created": b.get("CreationDate").isoformat() if b.get("CreationDate") else None,
                })
        except Exception as api_exc:
            logger.warning("buckets.list_api_failed", error=str(api_exc))
            # API failed and no fallback buckets configured
            raise api_exc

        logger.info("buckets.list", count=len(buckets), user=username)
        return {"buckets": buckets}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("buckets.list.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.get("/bucket-count")
def bucket_object_count(
    bucket: str = Query(..., description="Bucket name"),
    username: str = Depends(require_auth),
):
    """Return the total number of objects in a bucket."""
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        object_count = 0

        for page in paginator.paginate(Bucket=bucket, Prefix=""):
            object_count += len(page.get("Contents", []))

        logger.info("bucket.count", bucket=bucket, count=object_count, user=username)
        return {"bucket": bucket, "count": object_count}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("bucket.count.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
