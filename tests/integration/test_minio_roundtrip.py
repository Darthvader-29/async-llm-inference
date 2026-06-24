"""MinIO round-trip integration — the only Phase 4 test that needs infra.

Uses a *real* ``ThreadOffloader`` (asyncio.to_thread) and a *real* boto3 client
against the compose-managed MinIO, proving the full off-loop path works against
an actual S3-compatible server. Gated behind ``-m integration``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import boto3
import pytest
from botocore.config import Config

from app.adapters.object_store.s3 import S3ObjectStore
from app.core.concurrency import ThreadOffloader
from app.core.config import RetrySettings

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

pytestmark = pytest.mark.integration  # gated: only runs with `-m integration` + infra


@pytest.fixture
def s3_client() -> Iterator[S3Client]:
    """Real boto3 client pointed at compose MinIO (path-style, dev creds)."""
    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # REQUIRED for MinIO
            retries={"max_attempts": 0},  # tenacity owns retries
        ),
    )
    yield client
    client.close()


async def test_put_get_roundtrip_through_real_offloader(s3_client: S3Client) -> None:
    bucket = f"it-{uuid.uuid4().hex[:12]}"
    store = S3ObjectStore(
        s3_client,
        bucket,
        ThreadOffloader(),  # real to_thread offloading
        RetrySettings(max_attempts=3, base_delay_s=0.0, max_delay_s=0.0),
    )

    await store.ensure_bucket()  # creates the unique bucket on MinIO
    key = "results/roundtrip.json"
    payload = b'{"answer": "42", "ok": true}'

    ref = await store.put_bytes(key, payload, "application/json")
    assert ref == f"s3://{bucket}/{key}"

    fetched = await store.get_bytes(key)
    assert fetched == payload  # byte-for-byte round-trip off-loop

    # Idempotent ensure_bucket: calling again must not error.
    await store.ensure_bucket()
