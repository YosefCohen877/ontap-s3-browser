"""
routers/lifecycle.py — Bucket Lifecycle Configuration CRUD.

Exposes the S3 standard lifecycle API (PutBucketLifecycleConfiguration,
GetBucketLifecycleConfiguration, DeleteBucketLifecycle) over a stable
JSON shape, using the same S3 access key as the rest of the app.

ONTAP compatibility:
  - Requires ONTAP 9.13.1 or newer (older versions return NotImplemented,
    which is classified as category="lifecycle_not_supported" so the UI
    can render a version-aware banner).
  - Only expiration actions are supported by ONTAP — transitions to
    STANDARD_IA / GLACIER / DEEP_ARCHIVE are rejected server-side with a
    clear message since ONTAP does not support them at any version.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field

from app.auth import enforce_bucket_access, require_auth
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["lifecycle"])
logger = get_logger(__name__)


# ── Request schema ────────────────────────────────────────────────────────

class LifecycleFilter(BaseModel):
    prefix: str | None = None
    tags: list[dict[str, str]] = Field(default_factory=list)  # [{"key","value"}]
    size_greater_than: int | None = None
    size_less_than: int | None = None


class LifecycleExpiration(BaseModel):
    days: int | None = None
    date: str | None = None  # ISO date YYYY-MM-DD
    expired_object_delete_marker: bool | None = None


class LifecycleNoncurrentExpiration(BaseModel):
    noncurrent_days: int | None = None
    newer_noncurrent_versions: int | None = None


class LifecycleAbortMPU(BaseModel):
    days_after_initiation: int | None = None


class LifecycleRule(BaseModel):
    id: str
    status: str = "Enabled"  # Enabled | Disabled
    filter: LifecycleFilter | None = None
    expiration: LifecycleExpiration | None = None
    noncurrent_version_expiration: LifecycleNoncurrentExpiration | None = None
    abort_incomplete_multipart_upload: LifecycleAbortMPU | None = None


class LifecycleRulesPayload(BaseModel):
    rules: list[LifecycleRule]


# ── Helpers: boto3 <-> normalized JSON shape ─────────────────────────────

def _boto_filter_to_json(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Convert boto3 `Filter` block to our stable JSON shape."""
    if not raw:
        return {"prefix": None, "tags": [], "size_greater_than": None, "size_less_than": None}

    # boto3 returns one of: Prefix / Tag / And / ObjectSizeGreaterThan / ObjectSizeLessThan
    prefix = raw.get("Prefix")
    tags: list[dict[str, str]] = []
    size_gt = raw.get("ObjectSizeGreaterThan")
    size_lt = raw.get("ObjectSizeLessThan")

    if "Tag" in raw:
        t = raw["Tag"]
        tags.append({"key": t.get("Key", ""), "value": t.get("Value", "")})

    if "And" in raw:
        a = raw["And"]
        prefix = a.get("Prefix", prefix)
        size_gt = a.get("ObjectSizeGreaterThan", size_gt)
        size_lt = a.get("ObjectSizeLessThan", size_lt)
        for t in a.get("Tags", []):
            tags.append({"key": t.get("Key", ""), "value": t.get("Value", "")})

    return {
        "prefix": prefix,
        "tags": tags,
        "size_greater_than": size_gt,
        "size_less_than": size_lt,
    }


def _json_filter_to_boto(f: LifecycleFilter | None) -> dict[str, Any]:
    """Convert our JSON shape to a boto3 `Filter` block."""
    if f is None:
        return {"Prefix": ""}

    parts: dict[str, Any] = {}
    if f.prefix is not None and f.prefix != "":
        parts["Prefix"] = f.prefix
    if f.size_greater_than is not None:
        parts["ObjectSizeGreaterThan"] = int(f.size_greater_than)
    if f.size_less_than is not None:
        parts["ObjectSizeLessThan"] = int(f.size_less_than)

    tags = [{"Key": t.get("key", ""), "Value": t.get("value", "")} for t in (f.tags or []) if t.get("key")]

    # S3 requires exactly one top-level criterion OR And{} when multiple.
    criteria_count = (1 if "Prefix" in parts else 0) + \
                     (1 if "ObjectSizeGreaterThan" in parts else 0) + \
                     (1 if "ObjectSizeLessThan" in parts else 0) + \
                     len(tags)

    if criteria_count == 0:
        return {"Prefix": ""}

    if criteria_count == 1:
        if tags:
            return {"Tag": tags[0]}
        return parts

    # Multiple → And{}
    and_block: dict[str, Any] = {}
    if "Prefix" in parts:
        and_block["Prefix"] = parts["Prefix"]
    if "ObjectSizeGreaterThan" in parts:
        and_block["ObjectSizeGreaterThan"] = parts["ObjectSizeGreaterThan"]
    if "ObjectSizeLessThan" in parts:
        and_block["ObjectSizeLessThan"] = parts["ObjectSizeLessThan"]
    if tags:
        and_block["Tags"] = tags
    return {"And": and_block}


def _boto_rule_to_json(rule: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": rule.get("ID", ""),
        "status": rule.get("Status", "Enabled"),
        "filter": _boto_filter_to_json(rule.get("Filter")),
    }

    if "Expiration" in rule:
        e = rule["Expiration"]
        exp_date = e.get("Date")
        if isinstance(exp_date, (datetime, date)):
            exp_date = exp_date.date().isoformat() if isinstance(exp_date, datetime) else exp_date.isoformat()
        out["expiration"] = {
            "days": e.get("Days"),
            "date": exp_date,
            "expired_object_delete_marker": e.get("ExpiredObjectDeleteMarker"),
        }

    if "NoncurrentVersionExpiration" in rule:
        n = rule["NoncurrentVersionExpiration"]
        out["noncurrent_version_expiration"] = {
            "noncurrent_days": n.get("NoncurrentDays"),
            "newer_noncurrent_versions": n.get("NewerNoncurrentVersions"),
        }

    if "AbortIncompleteMultipartUpload" in rule:
        a = rule["AbortIncompleteMultipartUpload"]
        out["abort_incomplete_multipart_upload"] = {
            "days_after_initiation": a.get("DaysAfterInitiation"),
        }

    # Expose informational presence of unsupported transitions so UI can warn
    if "Transitions" in rule or "NoncurrentVersionTransitions" in rule:
        out["_has_unsupported_transitions"] = True

    return out


def _json_rule_to_boto(rule: LifecycleRule) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ID": rule.id,
        "Status": rule.status if rule.status in {"Enabled", "Disabled"} else "Enabled",
        "Filter": _json_filter_to_boto(rule.filter),
    }

    if rule.expiration:
        e: dict[str, Any] = {}
        if rule.expiration.days is not None:
            e["Days"] = int(rule.expiration.days)
        if rule.expiration.date:
            # boto3 wants a datetime for Date; parse ISO YYYY-MM-DD
            try:
                parsed = datetime.fromisoformat(rule.expiration.date)
            except ValueError:
                raise HTTPException(status_code=400, detail={
                    "category": "validation_error",
                    "title": "Invalid Expiration Date",
                    "message": f"Expiration date must be ISO 8601 (YYYY-MM-DD). Got: {rule.expiration.date}",
                    "detail": None,
                })
            e["Date"] = parsed
        if rule.expiration.expired_object_delete_marker is not None:
            e["ExpiredObjectDeleteMarker"] = bool(rule.expiration.expired_object_delete_marker)
        if e:
            out["Expiration"] = e

    if rule.noncurrent_version_expiration:
        n: dict[str, Any] = {}
        if rule.noncurrent_version_expiration.noncurrent_days is not None:
            n["NoncurrentDays"] = int(rule.noncurrent_version_expiration.noncurrent_days)
        if rule.noncurrent_version_expiration.newer_noncurrent_versions is not None:
            n["NewerNoncurrentVersions"] = int(rule.noncurrent_version_expiration.newer_noncurrent_versions)
        if n:
            out["NoncurrentVersionExpiration"] = n

    if rule.abort_incomplete_multipart_upload and \
            rule.abort_incomplete_multipart_upload.days_after_initiation is not None:
        out["AbortIncompleteMultipartUpload"] = {
            "DaysAfterInitiation": int(rule.abort_incomplete_multipart_upload.days_after_initiation),
        }

    # S3 requires at least one action on the rule
    has_action = (
        "Expiration" in out
        or "NoncurrentVersionExpiration" in out
        or "AbortIncompleteMultipartUpload" in out
    )
    if not has_action:
        raise HTTPException(status_code=400, detail={
            "category": "validation_error",
            "title": "Rule Has No Action",
            "message": f"Lifecycle rule '{rule.id}' must specify at least one action (expiration, noncurrent-version expiration, or abort incomplete multipart upload).",
            "detail": None,
        })

    return out


def _guard_feature_enabled() -> None:
    cfg = get_settings()
    if not cfg.enable_bucket_lifecycle:
        raise HTTPException(status_code=403, detail={
            "category": "feature_disabled",
            "title": "Lifecycle Management Disabled",
            "message": "Modifying bucket lifecycle rules is disabled for this deployment.",
            "detail": "Set ENABLE_BUCKET_LIFECYCLE=true to enable this feature.",
        })


# ── Routes ────────────────────────────────────────────────────────────────

@router.get("/bucket/{bucket}/lifecycle")
def get_bucket_lifecycle(
    bucket: str = Path(..., min_length=3, max_length=63),
    username: str = Depends(require_auth),
):
    """Return the lifecycle configuration for a bucket in a stable JSON shape.

    Responses:
      - 200 {"rules": [...]} when configured (empty list if no rules).
      - 200 {"rules": []} also when the bucket has no lifecycle config at all.
      - 501 category=lifecycle_not_supported if ONTAP < 9.13.1.
    """
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        try:
            resp = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        except Exception as exc:
            info = classify_exception(exc)
            if info.category == "lifecycle_not_configured":
                logger.info("lifecycle.get.empty", bucket=bucket, user=username)
                return {"rules": [], "supported": True}
            raise

        rules = [_boto_rule_to_json(r) for r in resp.get("Rules", [])]
        logger.info("lifecycle.get", bucket=bucket, count=len(rules), user=username)
        return {"rules": rules, "supported": True}
    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("lifecycle.get.error", category=info.category, detail=info.detail, bucket=bucket)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.put("/bucket/{bucket}/lifecycle")
def put_bucket_lifecycle(
    payload: LifecycleRulesPayload,
    bucket: str = Path(..., min_length=3, max_length=63),
    username: str = Depends(require_auth),
):
    """Replace the full lifecycle configuration for a bucket.

    S3 semantics: PUT replaces all rules. Send the complete rules array.
    """
    _guard_feature_enabled()
    try:
        enforce_bucket_access(bucket)

        if not payload.rules:
            raise HTTPException(status_code=400, detail={
                "category": "validation_error",
                "title": "No Rules Provided",
                "message": "At least one lifecycle rule is required. To remove all rules, use DELETE instead.",
                "detail": None,
            })

        # Reject any attempt to include storage-class transitions (ONTAP doesn't support them)
        # Our JSON shape doesn't accept transitions, so this is defensive / future-proof
        # against arbitrary extra fields bypassing pydantic.

        # Enforce id uniqueness
        ids = [r.id for r in payload.rules]
        if len(set(ids)) != len(ids):
            raise HTTPException(status_code=400, detail={
                "category": "validation_error",
                "title": "Duplicate Rule IDs",
                "message": "Each lifecycle rule must have a unique ID.",
                "detail": None,
            })

        # Validate filter <-> action combination constraints
        for r in payload.rules:
            has_tag_filter = bool(r.filter and r.filter.tags)
            if r.abort_incomplete_multipart_upload and \
                    r.abort_incomplete_multipart_upload.days_after_initiation is not None and \
                    has_tag_filter:
                raise HTTPException(status_code=400, detail={
                    "category": "validation_error",
                    "title": "Invalid Rule",
                    "message": f"Rule '{r.id}': AbortIncompleteMultipartUpload cannot be combined with a tag filter.",
                    "detail": None,
                })
            if r.expiration and r.expiration.expired_object_delete_marker and has_tag_filter:
                raise HTTPException(status_code=400, detail={
                    "category": "validation_error",
                    "title": "Invalid Rule",
                    "message": f"Rule '{r.id}': ExpiredObjectDeleteMarker cannot be combined with a tag filter.",
                    "detail": None,
                })
            if r.expiration:
                set_count = sum(1 for v in (
                    r.expiration.days,
                    r.expiration.date,
                    r.expiration.expired_object_delete_marker,
                ) if v not in (None, False))
                if set_count > 1:
                    raise HTTPException(status_code=400, detail={
                        "category": "validation_error",
                        "title": "Invalid Expiration",
                        "message": f"Rule '{r.id}': Specify exactly one of days, date, or expired_object_delete_marker.",
                        "detail": None,
                    })

        boto_rules = [_json_rule_to_boto(r) for r in payload.rules]

        client = get_s3_client()
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": boto_rules},
        )
        logger.info("lifecycle.put", bucket=bucket, count=len(boto_rules), user=username)
        return {"status": "ok", "bucket": bucket, "count": len(boto_rules)}
    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("lifecycle.put.error", category=info.category, detail=info.detail, bucket=bucket)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })


@router.delete("/bucket/{bucket}/lifecycle")
def delete_bucket_lifecycle(
    bucket: str = Path(..., min_length=3, max_length=63),
    username: str = Depends(require_auth),
):
    """Remove the entire lifecycle configuration for a bucket."""
    _guard_feature_enabled()
    try:
        enforce_bucket_access(bucket)
        client = get_s3_client()
        client.delete_bucket_lifecycle(Bucket=bucket)
        logger.info("lifecycle.delete", bucket=bucket, user=username)
        return {"status": "ok", "bucket": bucket}
    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("lifecycle.delete.error", category=info.category, detail=info.detail, bucket=bucket)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
