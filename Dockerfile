# ══════════════════════════════════════════════════════════════════════════════
# Dockerfile — ontap-s3-browser
#
# Stages:
#   1. Build layer: install Python deps into /install
#   2. Final layer: python:3.12-slim + inject internal root CA + copy app
#
# Custom CA injection:
#   Place *.crt or *.pem files in the ./certs/ directory before building.
#   They will be imported into the container's OS trust store automatically.
#   Set S3_CA_BUNDLE=/usr/local/share/ca-certificates/custom-ca.pem in .env
#   if ONTAP uses a CA not in the standard bundle.
# ══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build tools needed by some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: final runtime image ─────────────────────────────────────────────
FROM python:3.12-slim AS final

LABEL maintainer="ontap-s3-browser"
LABEL description="Self-hosted NetApp ONTAP S3 browser"

WORKDIR /app

# ── System packages for CA management ────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Inject internal root CA certificates (OPTIONAL) ──────────────────────────
# To add custom CA certificates:
#   1. Place your .crt or .pem files in the ./certs/ directory
#   2. Uncomment the COPY and RUN lines below
#   3. Set S3_CA_BUNDLE in .env if needed, or leave empty to use OS trust store
# COPY certs/ /usr/local/share/ca-certificates/ontap/
# RUN update-ca-certificates || true

# ── Python packages from builder ──────────────────────────────────────────────
COPY --from=builder /install /usr/local

# ── Application code ──────────────────────────────────────────────────────────
COPY app/       /app/app/
COPY frontend/  /app/frontend/

# ── Non-root user for security ────────────────────────────────────────────────
RUN useradd -r -s /bin/false appuser
USER appuser

# ── Runtime ───────────────────────────────────────────────────────────────────
EXPOSE 8080

# Use exec form so signals are handled properly (graceful shutdown)
CMD ["python", "-m", "uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--log-level", "warning"]
