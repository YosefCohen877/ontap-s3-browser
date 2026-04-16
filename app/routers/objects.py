"""
routers/objects.py — List, inspect, and download S3 objects.
"""
from __future__ import annotations

import mimetypes
import urllib.parse
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, File, Form, UploadFile
from fastapi.responses import StreamingResponse

from app.auth import require_auth, enforce_bucket_access
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["objects"])
logger = get_logger(__name__)

# ── streaming chunk size ─────────────────────────────────────────────────────
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


@router.get("/objects")
def list_objects(
    bucket: str = Query(..., description="Bucket name"),
    prefix: str = Query("", description="Key prefix (folder path)"),
    search: str = Query("", description="Filter results by substring"),
    sort: str = Query("name", description="Sort field: name | size | modified"),
    order: str = Query("asc", description="Sort order: asc | desc"),
    username: str = Depends(require_auth),
):
    """
    List objects and common prefixes (virtual folders) within a bucket/prefix.
    Returns a flat list with type=prefix (folder) or type=object (file).
    """
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        paginator = client.get_paginator("list_objects_v2")

        items = []
        page_iter = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        )

        for page in page_iter:
            # Virtual folders
            for cp in page.get("CommonPrefixes", []):
                folder_key = cp["Prefix"]
                name = folder_key[len(prefix):].rstrip("/")
                if search and search.lower() not in name.lower():
                    continue
                items.append({
                    "type": "prefix",
                    "key": folder_key,
                    "name": name,
                    "size": None,
                    "modified": None,
                    "content_type": None,
                    "etag": None,
                })

            # Objects
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == prefix:
                    continue  # skip the "folder itself" placeholder
                name = key[len(prefix):]
                if search and search.lower() not in name.lower():
                    continue
                items.append({
                    "type": "object",
                    "key": key,
                    "name": name,
                    "size": obj.get("Size"),
                    "modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                    "content_type": None,
                    "etag": obj.get("ETag", "").strip('"'),
                })

        # ── Sorting ───────────────────────────────────────────────────────
        reverse = order.lower() == "desc"
        sort_key = sort.lower()

        def _sort_fn(item):
            if sort_key == "size":
                return (item["size"] is None, item["size"] or 0)
            if sort_key == "modified":
                return (item["modified"] is None, item["modified"] or "")
            # default: name, folders first
            return (item["type"] == "object", item["name"].lower())

        items.sort(key=_sort_fn, reverse=reverse)

        logger.info(
            "objects.list",
            bucket=bucket,
            prefix=prefix,
            count=len(items),
            user=username,
        )
        return {"bucket": bucket, "prefix": prefix, "items": items}

    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.list.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.get("/object/meta")
def object_metadata(
    bucket: str = Query(...),
    key: str = Query(...),
    username: str = Depends(require_auth),
):
    """Return HEAD metadata for a single object."""
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        resp = client.head_object(Bucket=bucket, Key=key)
        meta = {
            "bucket": bucket,
            "key": key,
            "size": resp.get("ContentLength"),
            "modified": resp["LastModified"].isoformat() if resp.get("LastModified") else None,
            "content_type": resp.get("ContentType"),
            "etag": resp.get("ETag", "").strip('"'),
            "storage_class": resp.get("StorageClass"),
            "user_metadata": resp.get("Metadata", {}),
        }
        logger.info("objects.meta", bucket=bucket, key=key, user=username)
        return meta
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.meta.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.get("/object/download")
def download_object(
    bucket: str = Query(...),
    key: str = Query(...),
    username: str = Depends(require_auth),
):
    """Stream an object as a file download."""
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        resp = client.get_object(Bucket=bucket, Key=key)
        content_type = resp.get("ContentType", "application/octet-stream")
        filename = key.split("/")[-1] or "download"

        def _stream():
            body = resp["Body"]
            while True:
                chunk = body.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

        encoded_filename = urllib.parse.quote(filename)
        logger.info("objects.download", bucket=bucket, key=key, user=username)
        return StreamingResponse(
            _stream(),
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
                "Content-Length": str(resp.get("ContentLength", "")),
            },
        )
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.download.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.post("/object/upload")
def upload_object(
    bucket: str = Form(..., description="Bucket name"),
    prefix: str = Form("", description="Key prefix (folder path)"),
    file: UploadFile = File(..., description="File to upload"),
    username: str = Depends(require_auth),
):
    """Upload an object stream directly to S3."""
    cfg = get_settings()
    if not cfg.enable_upload:
        raise HTTPException(status_code=403, detail="Upload functionality is disabled")

    # prefix is like "some/folder/" or "". Ensure key builds correctly.
    key = file.filename
    if prefix:
        if not prefix.endswith("/"):
            prefix += "/"
        key = prefix + file.filename

    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        content_type = file.content_type or mimetypes.guess_type(file.filename)[0] or "application/octet-stream"

        extra_args = {"ContentType": content_type}
        logger.info("objects.upload.start", bucket=bucket, key=key, user=username, content_type=content_type)
        
        client.upload_fileobj(
            file.file,
            bucket,
            key,
            ExtraArgs=extra_args
        )
        
        logger.info("objects.upload.success", bucket=bucket, key=key, user=username)
        return {"status": "ok", "bucket": bucket, "key": key}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.upload.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.delete("/object")
def delete_object(
    bucket: str = Query(...),
    key: str = Query(...),
    username: str = Depends(require_auth),
):
    """Delete a single object from S3."""
    cfg = get_settings()
    if not cfg.enable_delete:
        raise HTTPException(status_code=403, detail="Delete functionality is disabled")

    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        logger.info("objects.delete.start", bucket=bucket, key=key, user=username)
        client.delete_object(Bucket=bucket, Key=key)
        logger.info("objects.delete.success", bucket=bucket, key=key, user=username)
        return {"status": "ok", "bucket": bucket, "key": key}
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.delete.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
