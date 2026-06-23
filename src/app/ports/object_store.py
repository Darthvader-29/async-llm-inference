"""Object-store port: S3-compatible blob storage.

Returns ``s3://bucket/key`` reference strings rather than presigned URLs or raw
clients, so the rest of the system stores a stable pointer in PostgreSQL.
"""

from __future__ import annotations

from typing import Protocol


class ObjectStore(Protocol):
    """Async S3-compatible object store, bound to a single bucket.

    The target bucket is fixed at construction time — the adapter is
    ``S3ObjectStore(client, bucket, offloader, retry)`` (Phase 4) — so no method
    takes a ``bucket`` argument. This matches the single-artifacts-bucket design
    and keeps every call site free of bucket bookkeeping.
    """

    async def ensure_bucket(self) -> None:
        """Create the configured bucket if it does not exist; no-op if it does.

        Called by the composition root in dev/test only (idempotent). In prod
        the bucket is assumed provisioned out-of-band.
        """
        ...

    async def bucket_exists(self) -> bool:
        """Return True if the configured bucket exists (non-mutating probe).

        Used by the readiness endpoint (Phase 6 ``GET /health/ready``) — a HEAD
        on the bucket, never a create.
        """
        ...

    async def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        """Store ``data`` at ``key`` in the bound bucket; return its ``s3://`` ref.

        Overwrites any existing object at the same key (last-write-wins). The
        returned ref (``f"s3://{bucket}/{key}"``) is what gets persisted as the
        job's ``result_ref``.
        """
        ...

    async def get_bytes(self, key: str) -> bytes:
        """Fetch the object at ``key`` (in the bound bucket) as bytes.

        Raises a transient/permanent upstream error per the adapter's
        classification if the fetch fails.
        """
        ...
