"""
routers/buckets.py — List S3 buckets.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth import enforce_bucket_access, require_auth
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["buckets"])
logger = get_logger(__name__)
COUNT_CACHE_TTL_SECONDS = 120
_bucket_count_cache: dict[str, tuple[int, float]] = {}

# Bucket-list cache: avoids re-running list_buckets + head_bucket on every request
_buckets_cache: dict | None = None
_buckets_cache_ts: float = 0.0
BUCKETS_CACHE_TTL_SECONDS = 30


@router.post("/bucket")
def create_bucket(
    bucket: str = Query(..., min_length=3, max_length=63, description="Bucket name to create"),
    username: str = Depends(require_auth),
):
    """Create a new S3 bucket."""
    cfg = get_settings()
    if not cfg.enable_create_bucket:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Bucket Creation Disabled",
            "message": "Bucket creation is disabled for this deployment.",
            "detail": "Set ENABLE_CREATE_BUCKET=true to enable this feature.",
        })
    if cfg.s3_forced_bucket:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Bucket Creation Disabled",
            "message": "Bucket creation is disabled in forced bucket mode.",
            "detail": f"This instance is locked to bucket: {cfg.s3_forced_bucket}",
        })

    try:
        client = get_s3_client()
        created = False
        create_err = None
        # ONTAP deployments can differ on whether LocationConstraint is required.
        create_variants = [
            {"Bucket": bucket},
            {
                "Bucket": bucket,
                "CreateBucketConfiguration": {"LocationConstraint": cfg.s3_region},
            },
        ]
        for kwargs in create_variants:
            try:
                client.create_bucket(**kwargs)
                created = True
                break
            except Exception as exc:
                create_err = exc

        if not created and create_err:
            raise create_err

        _bucket_count_cache.pop(bucket, None)
        _buckets_cache = None
        logger.info("bucket.create.success", bucket=bucket, user=username)
        return {"status": "ok", "bucket": bucket}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("bucket.create.error", category=info.category, detail=info.detail, bucket=bucket)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.get("/buckets")
def list_buckets(username: str = Depends(require_auth)):
    """Return all buckets accessible with the configured credentials."""
    global _buckets_cache, _buckets_cache_ts
    try:
        cfg = get_settings()

        if cfg.s3_forced_bucket:
            logger.info("buckets.list.forced", bucket=cfg.s3_forced_bucket, user=username)
            return {
                "buckets": [{"name": cfg.s3_forced_bucket, "created": None}],
                "forced": True
            }

        # Return server-side cache if fresh
        now = time.time()
        if _buckets_cache and (now - _buckets_cache_ts) <= BUCKETS_CACHE_TTL_SECONDS:
            logger.info("buckets.list.cache_hit", age=int(now - _buckets_cache_ts), user=username)
            return _buckets_cache

        client = get_s3_client()

        try:
            response = client.list_buckets()
        except Exception as api_exc:
            logger.warning("buckets.list_api_failed", error=str(api_exc))
            raise api_exc

        raw_buckets = response.get("Buckets", [])

        def _check_access(name: str) -> bool:
            try:
                client.head_bucket(Bucket=name)
                return True
            except Exception:
                return False

        # Parallel head_bucket — capped to connection pool size to avoid
        # overwhelming ONTAP's TLS handshake capacity.
        max_workers = min(len(raw_buckets), cfg.s3_max_pool_connections)
        access_map: dict[str, bool] = {}

        if max_workers > 0:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(_check_access, b["Name"]): b["Name"]
                    for b in raw_buckets
                }
                for future in as_completed(futures):
                    name = futures[future]
                    access_map[name] = future.result()

        buckets = [
            {
                "name": b["Name"],
                "created": b.get("CreationDate").isoformat() if b.get("CreationDate") else None,
                "accessible": access_map.get(b["Name"], False),
            }
            for b in raw_buckets
        ]

        accessible_count = sum(1 for b in buckets if b["accessible"])
        logger.info("buckets.list", count=len(buckets), accessible=accessible_count, user=username)

        result = {"buckets": buckets}
        _buckets_cache = result
        _buckets_cache_ts = time.time()

        return result
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
    refresh: bool = Query(False, description="Bypass count cache"),
    username: str = Depends(require_auth),
):
    """Return the total number of objects in a bucket."""
    cfg = get_settings()
    if not cfg.enable_bucket_count:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Bucket Count Disabled",
            "message": "File counting is disabled for this deployment.",
            "detail": "Set ENABLE_BUCKET_COUNT=true to enable this feature.",
        })
    try:
        enforce_bucket_access(bucket)
        now = time.time()
        cached = _bucket_count_cache.get(bucket)
        if cached and not refresh:
            cached_count, cached_at = cached
            age_seconds = now - cached_at
            if age_seconds <= COUNT_CACHE_TTL_SECONDS:
                logger.info("bucket.count.cache_hit", bucket=bucket, count=cached_count, age=int(age_seconds), user=username)
                return {"bucket": bucket, "count": cached_count, "cached": True}

        client = get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        object_count = 0

        for page in paginator.paginate(Bucket=bucket, Prefix=""):
            object_count += len(page.get("Contents", []))

        _bucket_count_cache[bucket] = (object_count, now)
        logger.info("bucket.count", bucket=bucket, count=object_count, user=username)
        return {"bucket": bucket, "count": object_count, "cached": False}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("bucket.count.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
