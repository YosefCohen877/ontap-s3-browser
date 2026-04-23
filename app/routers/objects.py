"""
routers/objects.py — List, inspect, and download S3 objects.
"""
from __future__ import annotations

import mimetypes
import os
import re
import urllib.parse
from typing import List, Optional

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query, File, Form, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from app.auth import require_auth, enforce_bucket_access
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["objects"])
logger = get_logger(__name__)

# ── streaming chunk size ─────────────────────────────────────────────────────
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

_MAX_TAGS_PER_OBJECT = 10
_MAX_TAG_KEY_LEN = 128
_MAX_TAG_VALUE_LEN = 256


class TagItem(BaseModel):
    key: str = Field(..., min_length=1, max_length=_MAX_TAG_KEY_LEN)
    value: str = Field(default="", max_length=_MAX_TAG_VALUE_LEN)

    @field_validator("key", mode="before")
    @classmethod
    def strip_key(cls, v):
        return v.strip() if isinstance(v, str) else v


class ObjectTagsPutBody(BaseModel):
    """Full tag set replacement (empty list clears all tags)."""

    tags: list[TagItem] = Field(default_factory=list)

    @field_validator("tags")
    @classmethod
    def cap_count(cls, v: list[TagItem]) -> list[TagItem]:
        if len(v) > _MAX_TAGS_PER_OBJECT:
            raise ValueError(f"at most {_MAX_TAGS_PER_OBJECT} tags allowed")
        return v


class TagsMergeBody(BaseModel):
    bucket: str
    keys: list[str] = Field(..., min_length=1, max_length=1000)
    tags: list[TagItem] = Field(..., min_length=1)

    @field_validator("tags")
    @classmethod
    def cap_merge_tags(cls, v: list[TagItem]) -> list[TagItem]:
        if len(v) > _MAX_TAGS_PER_OBJECT:
            raise ValueError(f"at most {_MAX_TAGS_PER_OBJECT} tags per merge request")
        return v


def _tag_set_from_items(items: list[TagItem]) -> list[dict[str, str]]:
    """Deduplicate by key (last wins), enforce S3 max tag count."""
    merged: dict[str, str] = {}
    for it in items:
        k = it.key.strip()
        if not k:
            continue
        if len(k) > _MAX_TAG_KEY_LEN or len(it.value) > _MAX_TAG_VALUE_LEN:
            raise HTTPException(
                status_code=400,
                detail={
                    "category": "validation_error",
                    "title": "Invalid tag",
                    "message": f"Tag keys must be 1–{_MAX_TAG_KEY_LEN} characters; values up to {_MAX_TAG_VALUE_LEN}.",
                    "detail": None,
                },
            )
        merged[k] = it.value
    if len(merged) > _MAX_TAGS_PER_OBJECT:
        raise HTTPException(
            status_code=400,
            detail={
                "category": "validation_error",
                "title": "Too many tags",
                "message": f"S3 allows at most {_MAX_TAGS_PER_OBJECT} tags per object.",
                "detail": None,
            },
        )
    return [{"Key": k, "Value": v} for k, v in sorted(merged.items())]


def _require_object_tagging(cfg) -> None:
    if not cfg.enable_object_tagging:
        raise HTTPException(
            status_code=403,
            detail={
                "category": "feature_disabled",
                "title": "Object tagging disabled",
                "message": "Object tagging is disabled on this server.",
                "detail": None,
            },
        )


@router.get("/objects")
def list_objects(
    bucket: str = Query(..., description="Bucket name"),
    prefix: str = Query("", description="Key prefix (folder path)"),
    search: str = Query("", description="Filter results by substring"),
    sort: str = Query("name", description="Sort field: name | size | modified"),
    order: str = Query("asc", description="Sort order: asc | desc"),
    page_size: int = Query(20, ge=5, le=200, description="Items per page"),
    continuation_token: Optional[str] = Query(None, description="Continuation token for paging"),
    username: str = Depends(require_auth),
):
    """
    List objects and common prefixes (virtual folders) within a bucket/prefix.
    Returns a flat list with type=prefix (folder) or type=object (file).
    """
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        items = []
        next_token = continuation_token
        scanned_pages = 0

        # Fetch until we fill one UI page with filtered results (or hit end).
        while len(items) < page_size and scanned_pages < 20:
            kwargs = {
                "Bucket": bucket,
                "Prefix": prefix,
                "Delimiter": "/",
                "MaxKeys": max(1, page_size - len(items)),
            }
            if next_token:
                kwargs["ContinuationToken"] = next_token

            page = client.list_objects_v2(**kwargs)
            scanned_pages += 1

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
                if len(items) >= page_size:
                    break

            if len(items) < page_size:
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
                    if len(items) >= page_size:
                        break

            next_token = page.get("NextContinuationToken")
            if not next_token:
                break

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
        items = items[:page_size]

        logger.info(
            "objects.list",
            bucket=bucket,
            prefix=prefix,
            count=len(items),
            page_size=page_size,
            has_more=bool(next_token),
            user=username,
        )
        return {
            "bucket": bucket,
            "prefix": prefix,
            "items": items,
            "next_token": next_token,
            "has_more": bool(next_token),
        }

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
        tags: list[dict[str, str]] = []
        try:
            tag_resp = client.get_object_tagging(Bucket=bucket, Key=key)
            tags = [
                {"key": t["Key"], "value": t["Value"]}
                for t in tag_resp.get("TagSet", [])
            ]
        except ClientError as exc:
            logger.debug(
                "objects.meta.tags_skip",
                bucket=bucket,
                key=key,
                code=exc.response.get("Error", {}).get("Code", ""),
            )
        except Exception as exc:
            logger.debug("objects.meta.tags_skip", bucket=bucket, key=key, error=str(exc))

        meta = {
            "bucket": bucket,
            "key": key,
            "size": resp.get("ContentLength"),
            "modified": resp["LastModified"].isoformat() if resp.get("LastModified") else None,
            "content_type": resp.get("ContentType"),
            "etag": resp.get("ETag", "").strip('"'),
            "tags": tags,
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


@router.put("/object/tags")
def put_object_tags(
    body: ObjectTagsPutBody,
    bucket: str = Query(...),
    key: str = Query(...),
    username: str = Depends(require_auth),
):
    """Replace all S3 object tags (empty list clears tags)."""
    cfg = get_settings()
    _require_object_tagging(cfg)
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        tag_set = _tag_set_from_items(body.tags)
        client.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tag_set})
        logger.info("objects.tags.put", bucket=bucket, key=key, count=len(tag_set), user=username)
        return {"status": "ok", "bucket": bucket, "key": key, "tags": [{"key": t["Key"], "value": t["Value"]} for t in tag_set]}
    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.tags.put.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.post("/objects/tags-merge")
def merge_object_tags_bulk(
    body: TagsMergeBody,
    username: str = Depends(require_auth),
):
    """Merge tag key/values into each object (existing keys overwritten)."""
    cfg = get_settings()
    _require_object_tagging(cfg)
    merge_dict = _tag_set_from_items(body.tags)
    merge_pairs = {t["Key"]: t["Value"] for t in merge_dict}
    errors: list[dict] = []
    updated = 0
    try:
        enforce_bucket_access(body.bucket)
        client = get_s3_client()
        for object_key in dict.fromkeys(body.keys):
            try:
                existing: dict[str, str] = {}
                try:
                    tr = client.get_object_tagging(Bucket=body.bucket, Key=object_key)
                    existing = {t["Key"]: t["Value"] for t in tr.get("TagSet", [])}
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    if code == "NoSuchKey":
                        errors.append({"key": object_key, "message": "Object not found"})
                        continue
                    if code == "NoSuchTagSet":
                        existing = {}
                    else:
                        errors.append({
                            "key": object_key,
                            "message": exc.response.get("Error", {}).get("Message", code),
                        })
                        continue
                merged = {**existing, **merge_pairs}
                if len(merged) > _MAX_TAGS_PER_OBJECT:
                    errors.append({
                        "key": object_key,
                        "message": f"Would exceed {_MAX_TAGS_PER_OBJECT} tags (has {len(existing)}, merge adds conflicts).",
                    })
                    continue
                tag_set = [{"Key": k, "Value": v} for k, v in sorted(merged.items())]
                client.put_object_tagging(
                    Bucket=body.bucket,
                    Key=object_key,
                    Tagging={"TagSet": tag_set},
                )
                updated += 1
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                errors.append({"key": object_key, "message": f"{code}: {exc.response.get('Error', {}).get('Message', '')}"})
            except Exception as exc:
                errors.append({"key": object_key, "message": str(exc)})
        logger.info(
            "objects.tags.merge_bulk",
            bucket=body.bucket,
            updated=updated,
            errors=len(errors),
            user=username,
        )
        return {"status": "ok", "bucket": body.bucket, "updated": updated, "errors": errors}
    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.tags.merge.error", category=info.category, detail=info.detail)
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

    # Sanitize filename: strip path components and control characters
    safe_name = os.path.basename(file.filename or "upload")
    safe_name = re.sub(r'[\x00-\x1f]', '', safe_name).strip('. ')
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    key = safe_name
    if prefix:
        if not prefix.endswith("/"):
            prefix += "/"
        key = prefix + safe_name

    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        content_type = file.content_type or mimetypes.guess_type(safe_name)[0] or "application/octet-stream"

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


class BulkDeleteRequest(BaseModel):
    bucket: str
    keys: List[str]


@router.post("/objects/delete-bulk")
def delete_objects_bulk(
    body: BulkDeleteRequest,
    username: str = Depends(require_auth),
):
    """Delete multiple objects from S3 in a single request."""
    cfg = get_settings()
    if not cfg.enable_delete:
        raise HTTPException(status_code=403, detail="Delete functionality is disabled")

    if not body.keys:
        raise HTTPException(status_code=400, detail="No keys provided")

    if len(body.keys) > 1000:
        raise HTTPException(status_code=400, detail="Cannot delete more than 1000 objects at once")

    try:
        enforce_bucket_access(body.bucket)
        client = get_s3_client()
        logger.info(
            "objects.delete_bulk.start",
            bucket=body.bucket,
            count=len(body.keys),
            user=username,
        )

        delete_objects = [{"Key": k} for k in body.keys]
        resp = client.delete_objects(
            Bucket=body.bucket,
            Delete={"Objects": delete_objects, "Quiet": True},
        )

        errors = resp.get("Errors", [])
        deleted_count = len(body.keys) - len(errors)

        logger.info(
            "objects.delete_bulk.done",
            bucket=body.bucket,
            deleted=deleted_count,
            errors=len(errors),
            user=username,
        )
        return {
            "status": "ok",
            "bucket": body.bucket,
            "deleted": deleted_count,
            "errors": [
                {"key": e.get("Key"), "code": e.get("Code"), "message": e.get("Message")}
                for e in errors
            ],
        }
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("objects.delete_bulk.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
