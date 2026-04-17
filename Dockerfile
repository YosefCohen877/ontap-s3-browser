# ══════════════════════════════════════════════════════════════════════════════
# Dockerfile — ontap-s3-browser
#
# Stages:
#   1. Build layer: install Python deps, strip unused boto3 services
#   2. Final layer: python:3.12-alpine + inject internal root CA + copy app
#
# Custom CA injection:
#   Place *.crt or *.pem files in the ./certs/ directory before building.
#   They will be imported into the container's OS trust store automatically.
#   Set S3_CA_BUNDLE=/usr/local/share/ca-certificates/custom-ca.pem in .env
#   if ONTAP uses a CA not in the standard bundle.
# ══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-alpine AS builder

WORKDIR /build

RUN apk add --no-cache gcc musl-dev

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir --no-compile -r requirements.txt

# Strip botocore service models we don't need (keep only S3)
RUN find /install/lib/python*/site-packages/botocore/data/ \
    -mindepth 1 -maxdepth 1 -type d \
    ! -name s3 ! -name s3control \
    -exec rm -rf {} + \
    && find /install/lib/python*/site-packages -type d -name __pycache__ -exec rm -rf {} + \
    && find /install/lib/python*/site-packages -name '*.pyc' -delete \
    && find /install/lib/python*/site-packages -name '*.pyo' -delete


# ── Stage 2: final runtime image ─────────────────────────────────────────────
FROM python:3.12-alpine AS final

LABEL maintainer="ontap-s3-browser"
LABEL description="Self-hosted NetApp ONTAP S3 browser"

WORKDIR /app

# ── System packages for CA management ────────────────────────────────────────
RUN apk add --no-cache ca-certificates

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
RUN adduser -D -s /bin/false appuser
USER appuser

# ── Runtime ───────────────────────────────────────────────────────────────────
EXPOSE 8080

# Use exec form so signals are handled properly (graceful shutdown)
CMD ["python", "-m", "uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--log-level", "warning"]
