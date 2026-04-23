"""
routers/preview.py — Return a size-limited preview of text/JSON/log objects.
Limits come from Settings (PREVIEW_MAX_TEXT_BYTES, PREVIEW_MAX_BINARY_BYTES).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse, Response

from app.auth import require_auth, enforce_bucket_access
from app.config import get_settings
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["preview"])
logger = get_logger(__name__)

# File types we can render in the browser
TEXT_TYPES = {
    "text/plain", "text/csv", "text/html", "text/xml", "application/xml",
    "application/json", "application/x-ndjson",
}
TEXT_EXTENSIONS = {
    ".txt", ".log", ".json", ".yaml", ".yml", ".csv", ".xml",
    ".conf", ".cfg", ".ini", ".sh", ".py", ".js", ".md", ".sql",
    ".properties", ".env", ".toml",
}
IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp", "image/svg+xml"}
VIDEO_TYPES = {"video/mp4", "video/webm", "video/ogg"}
VIDEO_EXTENSIONS = {".mp4", ".webm", ".ogv", ".ogg"}
PDF_TYPE = "application/pdf"


def _too_large_message(kind: str, size: int, limit: int) -> str:
    sz_mb = size / (1024 * 1024)
    lim_mb = limit / (1024 * 1024)
    return f"{kind} is {sz_mb:.1f} MB; max preview size is {lim_mb:.1f} MB — download instead."


def _is_text(content_type: str, key: str) -> bool:
    if content_type:
        base = content_type.split(";")[0].strip().lower()
        if base in TEXT_TYPES or base.startswith("text/"):
            return True
    ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
    return ext in TEXT_EXTENSIONS


def _is_image(content_type: str) -> bool:
    base = (content_type or "").split(";")[0].strip().lower()
    return base in IMAGE_TYPES


def _is_video(content_type: str, key: str) -> bool:
    base = (content_type or "").split(";")[0].strip().lower()
    if base in VIDEO_TYPES:
        return True
    ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
    return ext in VIDEO_EXTENSIONS


def _is_pdf(content_type: str) -> bool:
    base = (content_type or "").split(";")[0].strip().lower()
    return base == PDF_TYPE


@router.get("/object/preview")
def preview_object(
    bucket: str = Query(...),
    key: str = Query(...),
    username: str = Depends(require_auth),
):
    """
    Return an object for in-browser preview.
    - Text/JSON/log: return raw bytes (limited to preview_max_text_bytes)
    - Image/PDF/video: stream inline up to preview_max_binary_bytes
    - Other: return 415 Unsupported Media Type
    """
    try:
        cfg = get_settings()
        text_limit = cfg.preview_max_text_bytes
        binary_limit = cfg.preview_max_binary_bytes

        enforce_bucket_access(bucket)
        client = get_s3_client()
        head = client.head_object(Bucket=bucket, Key=key)
        size = head.get("ContentLength", 0)
        content_type = head.get("ContentType", "application/octet-stream")

        logger.info(
            "preview.request",
            bucket=bucket,
            key=key,
            size=size,
            content_type=content_type,
            user=username,
        )

        # ── Text preview ──────────────────────────────────────────────────
        if _is_text(content_type, key):
            read_bytes = min(size, text_limit)
            range_header = f"bytes=0-{read_bytes - 1}" if read_bytes > 0 else None

            get_kwargs = {"Bucket": bucket, "Key": key}
            if range_header:
                get_kwargs["Range"] = range_header

            resp = client.get_object(**get_kwargs)
            data = resp["Body"].read()
            truncated = size > text_limit

            # Try to decode; fall back gracefully
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = data.decode("latin-1", errors="replace")

            return {
                "preview_type": "text",
                "content": text,
                "truncated": truncated,
                "preview_text_limit_bytes": text_limit,
                "size": size,
                "content_type": content_type,
            }

        # ── Image inline stream ───────────────────────────────────────────
        if _is_image(content_type):
            if size > binary_limit:
                raise HTTPException(status_code=413, detail={
                    "category": "preview_too_large",
                    "title": "File Too Large for Preview",
                    "message": _too_large_message("Image", size, binary_limit),
                    "detail": None,
                })
            resp = client.get_object(Bucket=bucket, Key=key)

            def _stream():
                body = resp["Body"]
                while chunk := body.read(1024 * 1024):
                    yield chunk

            return StreamingResponse(
                _stream(),
                media_type=content_type,
                headers={"Content-Length": str(size), "Content-Disposition": "inline"},
            )

        # ── PDF inline stream ─────────────────────────────────────────────
        if _is_pdf(content_type):
            if size > binary_limit:
                raise HTTPException(status_code=413, detail={
                    "category": "preview_too_large",
                    "title": "File Too Large for Preview",
                    "message": _too_large_message("PDF", size, binary_limit),
                    "detail": None,
                })
            resp = client.get_object(Bucket=bucket, Key=key)

            def _pdf_stream():
                body = resp["Body"]
                while chunk := body.read(1024 * 1024):
                    yield chunk

            return StreamingResponse(
                _pdf_stream(),
                media_type="application/pdf",
                headers={"Content-Length": str(size), "Content-Disposition": "inline"},
            )

        # ── Video inline stream ────────────────────────────────────────────
        if _is_video(content_type, key):
            if size > binary_limit:
                raise HTTPException(status_code=413, detail={
                    "category": "preview_too_large",
                    "title": "File Too Large for Preview",
                    "message": _too_large_message("Video", size, binary_limit),
                    "detail": None,
                })
            resp = client.get_object(Bucket=bucket, Key=key)
            media = content_type if content_type.split(";")[0].strip().lower() in VIDEO_TYPES else "video/mp4"

            def _video_stream():
                body = resp["Body"]
                while chunk := body.read(1024 * 1024):
                    yield chunk

            return StreamingResponse(
                _video_stream(),
                media_type=media,
                headers={"Content-Length": str(size), "Content-Disposition": "inline"},
            )

        # ── Unsupported ───────────────────────────────────────────────────
        raise HTTPException(status_code=415, detail={
            "category": "preview_unsupported",
            "title": "Preview Not Available",
            "message": f"Preview is not supported for content type: {content_type}",
            "detail": None,
        })

    except HTTPException:
        raise
    except Exception as exc:
        info = classify_exception(exc)
        logger.error("preview.error", category=info.category, detail=info.detail)
        raise HTTPException(status_code=info.http_code, detail={
            "category": info.category,
            "title": info.title,
            "message": info.message,
            "detail": info.detail,
        })
