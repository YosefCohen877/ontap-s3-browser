"""
routers/buckets.py — List S3 buckets.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from botocore.exceptions import ClientError
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


def _delete_objects_batch(client, bucket: str, objects: list[dict]) -> None:
    if not objects:
        return
    for i in range(0, len(objects), 1000):
        chunk = objects[i : i + 1000]
        resp = client.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
        errs = resp.get("Errors") or []
        if errs:
            first = errs[0]
            raise RuntimeError(
                f"delete_objects failed for {first.get('Key')}: {first.get('Code')} — {first.get('Message')}"
            )


def _purge_bucket_contents(client, bucket: str) -> None:
    """
    Remove every object version, delete marker, and current key; abort multipart uploads.
    Falls back to list_objects_v2 only if list_object_versions is not supported.
    """
    use_versions = True
    try:
        client.list_object_versions(Bucket=bucket, MaxKeys=1)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NotImplemented", "NotSupported", "UnsupportedOperation", "InvalidArgument", "AccessNotSupported"):
            use_versions = False
            logger.info("bucket.purge.list_versions_unavailable", bucket=bucket, code=code)
        else:
            raise

    if use_versions:
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            objs: list[dict] = []
            for v in page.get("Versions") or []:
                objs.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers") or []:
                objs.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            _delete_objects_batch(client, bucket, objs)
    else:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            contents = page.get("Contents") or []
            objs = [{"Key": o["Key"]} for o in contents]
            _delete_objects_batch(client, bucket, objs)

    try:
        mpu = client.get_paginator("list_multipart_uploads")
        for page in mpu.paginate(Bucket=bucket):
            for up in page.get("Uploads") or []:
                client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=up["Key"],
                    UploadId=up["UploadId"],
                )
    except Exception as exc:
        logger.warning("bucket.purge.multipart_abort", bucket=bucket, error=str(exc))


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
        global _buckets_cache
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


@router.delete("/bucket")
def delete_bucket(
    bucket: str = Query(..., min_length=3, max_length=63, description="Bucket name to delete"),
    purge_contents: bool = Query(
        False,
        description="If true, delete all objects/versions first, then remove the bucket",
    ),
    username: str = Depends(require_auth),
):
    """Delete an S3 bucket (empty only unless purge_contents is true)."""
    cfg = get_settings()
    if not cfg.enable_delete_bucket:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Bucket Deletion Disabled",
            "message": "Deleting buckets is disabled for this deployment.",
            "detail": "Set ENABLE_DELETE_BUCKET=true to enable this feature.",
        })
    if cfg.s3_forced_bucket:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Bucket Deletion Disabled",
            "message": "Bucket deletion is disabled in forced bucket mode.",
            "detail": f"This instance is locked to bucket: {cfg.s3_forced_bucket}",
        })

    enforce_bucket_access(bucket)

    try:
        global _buckets_cache
        client = get_s3_client()
        if purge_contents:
            logger.info("bucket.delete.purge_start", bucket=bucket, user=username)
            try:
                _purge_bucket_contents(client, bucket)
            except Exception as exc:
                info = classify_exception(exc)
                logger.error("bucket.delete.purge_failed", bucket=bucket, category=info.category)
                raise HTTPException(status_code=info.http_code, detail={
                    "category": info.category,
                    "title": "Failed to empty bucket",
                    "message": info.message,
                    "detail": info.detail,
                }) from exc
            logger.info("bucket.delete.purge_done", bucket=bucket, user=username)

        client.delete_bucket(Bucket=bucket)
        _buckets_cache = None
        _bucket_count_cache.pop(bucket, None)
        logger.info("bucket.delete.success", bucket=bucket, user=username, purged=purge_contents)
        return {"status": "ok", "bucket": bucket, "purged": purge_contents}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("bucket.delete.error", category=info.category, detail=info.detail, bucket=bucket)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.get("/buckets")
def list_buckets(
    refresh: bool = Query(False, description="Bypass server-side bucket list cache"),
    username: str = Depends(require_auth),
):
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
        if (
            not refresh
            and _buckets_cache
            and (now - _buckets_cache_ts) <= BUCKETS_CACHE_TTL_SECONDS
        ):
            logger.info("buckets.list.cache_hit", age=int(now - _buckets_cache_ts), user=username)
            return _buckets_cache
        if refresh:
            logger.info("buckets.list.cache_bypass", user=username)

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
