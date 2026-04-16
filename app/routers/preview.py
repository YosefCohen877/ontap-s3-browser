"""
routers/preview.py — Return a size-limited preview of text/JSON/log objects.
Large binaries and images return metadata only; the frontend handles display.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse, Response

from app.auth import require_auth, enforce_bucket_access
from app.s3_client import get_s3_client
from app.utils.errors import classify_exception
from app.utils.logging import get_logger

router = APIRouter(prefix="/api", tags=["preview"])
logger = get_logger(__name__)

# Maximum bytes to load for text preview to avoid memory issues
TEXT_PREVIEW_LIMIT = 512 * 1024  # 512 KB
# Max size to inline-stream for images and PDFs
BINARY_PREVIEW_LIMIT = 20 * 1024 * 1024  # 20 MB

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
PDF_TYPE = "application/pdf"


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
    - Text/JSON/log: return raw bytes (limited to TEXT_PREVIEW_LIMIT)
    - Image/PDF: stream inline up to BINARY_PREVIEW_LIMIT
    - Other: return 415 Unsupported Media Type
    """
    try:
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
            read_bytes = min(size, TEXT_PREVIEW_LIMIT)
            range_header = f"bytes=0-{read_bytes - 1}" if read_bytes > 0 else None

            get_kwargs = {"Bucket": bucket, "Key": key}
            if range_header:
                get_kwargs["Range"] = range_header

            resp = client.get_object(**get_kwargs)
            data = resp["Body"].read()
            truncated = size > TEXT_PREVIEW_LIMIT

            # Try to decode; fall back gracefully
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = data.decode("latin-1", errors="replace")

            return {
                "preview_type": "text",
                "content": text,
                "truncated": truncated,
                "size": size,
                "content_type": content_type,
            }

        # ── Image inline stream ───────────────────────────────────────────
        if _is_image(content_type):
            if size > BINARY_PREVIEW_LIMIT:
                raise HTTPException(status_code=413, detail={
                    "category": "preview_too_large",
                    "title": "File Too Large for Preview",
                    "message": f"Image is {size // (1024*1024)} MB — download it instead.",
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
            if size > BINARY_PREVIEW_LIMIT:
                raise HTTPException(status_code=413, detail={
                    "category": "preview_too_large",
                    "title": "File Too Large for Preview",
                    "message": f"PDF is {size // (1024*1024)} MB — download it instead.",
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
