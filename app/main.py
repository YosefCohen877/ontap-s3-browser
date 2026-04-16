"""
main.py — FastAPI application factory.

Wires together:
  - Structured logging (configured first)
  - All API routers
  - Static file serving for the frontend
  - Startup validation (config & S3 client warm-up)
  - CORS (locked down by default — reverse proxy handles it)
"""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.routers import buckets, diagnostics, objects, preview
from app.utils.logging import configure_logging, get_logger

# ── Bootstrap logging first so all import-time log calls work ─────────────
cfg = get_settings()
configure_logging(cfg.log_level)
logger = get_logger(__name__)

# ── App factory ───────────────────────────────────────────────────────────
app = FastAPI(
    title="ONTAP S3 Browser",
    description="Self-hosted browser for NetApp ONTAP S3 — enterprise-grade.",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# ── CORS — tightly restricted; add origins if needed ─────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Override if exposing publicly; reverse proxy is preferred
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── API routers ───────────────────────────────────────────────────────────
app.include_router(diagnostics.router)
app.include_router(buckets.router)
app.include_router(objects.router)
app.include_router(preview.router)

# ── Serve frontend static files ───────────────────────────────────────────
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
    logger.info("frontend.mounted", path=str(FRONTEND_DIR))

    # Catch-all route for SPA routing (must be after mount and routers)
    from fastapi.responses import FileResponse
    @app.get("/{path_name:path}")
    async def catch_all(path_name: str):
        index_file = FRONTEND_DIR / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return {"error": "index.html not found"}
else:
    logger.warning("frontend.missing", path=str(FRONTEND_DIR))


# ── Startup event ─────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    logger.info(
        "app.startup",
        endpoint=cfg.s3_endpoint_url,
        region=cfg.s3_region,
        addressing_style=cfg.s3_addressing_style,
        tls_verify=cfg.s3_verify_ssl,
        port=cfg.app_port,
        log_level=cfg.log_level,
    )
    # Warm-up: pre-create the S3 client singleton so first request is fast
    try:
        from app.s3_client import get_s3_client
        get_s3_client()
        logger.info("app.startup.s3_client_ready")
    except Exception as exc:
        logger.warning(
            "app.startup.s3_client_warn",
            error=str(exc),
            message="S3 client could not be pre-warmed — will retry on first request",
        )


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("app.shutdown")
