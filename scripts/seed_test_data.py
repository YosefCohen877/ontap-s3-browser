"""
seed_test_data.py — Bulk-upload test files to an S3 bucket for testing.

Usage:
    python scripts/seed_test_data.py                          # 50 files to first accessible bucket
    python scripts/seed_test_data.py --bucket my-bucket       # 50 files to specific bucket
    python scripts/seed_test_data.py --count 200              # 200 files
    python scripts/seed_test_data.py --prefix test-data/      # upload into a folder
    python scripts/seed_test_data.py --cleanup                # delete all test files created by this script

Reads S3 credentials from your .env file automatically.
"""
import argparse
import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Add project root to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_settings
from app.s3_client import create_s3_client

TEST_PREFIX = "__test-seed__/"


def generate_file(index: int, size_kb: int = 1) -> tuple[str, bytes]:
    """Generate a test file with realistic content."""
    now = datetime.now(timezone.utc).isoformat()
    if index % 5 == 0:
        content = json.dumps({
            "id": index,
            "type": "log",
            "timestamp": now,
            "message": f"Test log entry #{index}",
            "level": "info",
            "data": {"value": index * 3.14, "tags": [f"tag-{i}" for i in range(5)]},
        }, indent=2).encode()
        ext = "json"
    elif index % 5 == 1:
        lines = [f"Line {j}: data-{index}-{j} | ts={now}" for j in range(max(1, size_kb * 20))]
        content = "\n".join(lines).encode()
        ext = "csv"
    elif index % 5 == 2:
        content = f"# Report #{index}\n\nGenerated at {now}\n\n{'Lorem ipsum. ' * size_kb * 10}".encode()
        ext = "md"
    elif index % 5 == 3:
        content = f"<?xml version='1.0'?>\n<record id='{index}' ts='{now}'>\n  <data>{'x' * size_kb * 100}</data>\n</record>".encode()
        ext = "xml"
    else:
        content = os.urandom(size_kb * 1024)
        ext = "bin"

    filename = f"file-{index:05d}.{ext}"
    return filename, content


def find_bucket(client) -> str:
    """Find the first accessible bucket."""
    response = client.list_buckets()
    for b in response.get("Buckets", []):
        name = b["Name"]
        try:
            client.head_bucket(Bucket=name)
            return name
        except Exception:
            continue
    print("ERROR: No accessible bucket found.")
    sys.exit(1)


def upload_batch(client, bucket: str, prefix: str, count: int, size_kb: int, workers: int):
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    full_prefix = prefix + TEST_PREFIX

    print(f"\nUploading {count} test files to s3://{bucket}/{full_prefix}")
    print(f"  File size: ~{size_kb} KB each")
    print(f"  Workers:   {workers}")
    print()

    uploaded = 0
    failed = 0
    start = time.time()

    def _upload_one(index: int):
        filename, content = generate_file(index, size_kb)
        key = full_prefix + filename
        client.put_object(Bucket=bucket, Key=key, Body=content)
        return key

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_upload_one, i): i for i in range(count)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                key = future.result()
                uploaded += 1
                if uploaded % 10 == 0 or uploaded == count:
                    elapsed = time.time() - start
                    rate = uploaded / elapsed if elapsed > 0 else 0
                    print(f"  [{uploaded}/{count}] {rate:.1f} files/sec", end="\r")
            except Exception as e:
                failed += 1
                print(f"\n  FAILED file-{idx:05d}: {e}")

    elapsed = time.time() - start
    print(f"\n\nDone! {uploaded} uploaded, {failed} failed in {elapsed:.1f}s ({uploaded/elapsed:.1f} files/sec)")


def cleanup(client, bucket: str, prefix: str):
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    full_prefix = prefix + TEST_PREFIX

    print(f"\nCleaning up test files from s3://{bucket}/{full_prefix}")

    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    if not keys:
        print("  No test files found.")
        return

    print(f"  Found {len(keys)} test files. Deleting...")

    # S3 delete_objects supports up to 1000 keys per call
    for i in range(0, len(keys), 1000):
        batch = keys[i:i+1000]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
        )

    print(f"  Deleted {len(keys)} test files.")


def main():
    parser = argparse.ArgumentParser(description="Seed test data into S3 bucket")
    parser.add_argument("--bucket", type=str, default=None, help="Target bucket (default: first accessible)")
    parser.add_argument("--prefix", type=str, default="", help="Key prefix / folder (e.g. 'logs/')")
    parser.add_argument("--count", type=int, default=50, help="Number of files to upload (default: 50)")
    parser.add_argument("--size", type=int, default=1, help="Approximate file size in KB (default: 1)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel upload threads (default: 4)")
    parser.add_argument("--cleanup", action="store_true", help="Delete all test files instead of uploading")
    args = parser.parse_args()

    cfg = get_settings()
    client = create_s3_client(cfg)
    bucket = args.bucket or cfg.s3_forced_bucket or find_bucket(client)

    print(f"Endpoint: {cfg.s3_endpoint_url}")
    print(f"Bucket:   {bucket}")

    if args.cleanup:
        cleanup(client, bucket, args.prefix)
    else:
        upload_batch(client, bucket, args.prefix, args.count, args.size, args.workers)


if __name__ == "__main__":
    main()
