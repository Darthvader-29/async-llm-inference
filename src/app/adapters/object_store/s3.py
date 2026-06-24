"""``S3ObjectStore`` — the ObjectStore port over a synchronous boto3 S3 client.

Discipline: NO boto3 call ever runs on the event loop. Every operation is
``retrying(...) -> offloader.run(self._client.<op>, ...)``; retry is the outer
loop, offload the inner action, and any raw botocore error is translated by
``classify_botocore_error`` before it reaches the retry predicate. The client is
constructed and closed by the composition root, never here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from app.adapters.object_store.errors import classify_botocore_error
from app.core.retry import retrying
from app.domain.exceptions import TransientUpstreamError

if TYPE_CHECKING:
    # boto3-stubs[s3] provides a precise client type for mypy --strict.
    from mypy_boto3_s3.client import S3Client

    from app.core.config import RetrySettings
    from app.ports.offloader import SyncOffloader


class S3ObjectStore:
    """ObjectStore implemented over a synchronous boto3 S3 client.

    Conforms structurally to ``app.ports.object_store.ObjectStore`` — it does
    not import the Protocol; mypy --strict checks conformance where the
    AppContainer (Phase 6) injects it.
    """

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        offloader: SyncOffloader,
        retry: RetrySettings,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._offload = offloader
        self._retry = retry

    # ---- public ObjectStore API -------------------------------------------

    async def ensure_bucket(self) -> None:
        """Create the bucket if absent. Idempotent. Dev-only (called by container)."""
        if await self.bucket_exists():
            return
        await self._call(self._client.create_bucket, Bucket=self._bucket)

    async def bucket_exists(self) -> bool:
        """head_bucket -> True; HTTP 404 -> False; anything else -> classified raise.

        Branches on the HTTP **status code**, not an exception class: a HEAD on a
        missing/forbidden bucket returns a bodyless 400/403/404 with no typed
        error to catch. A transient blip during the probe is surfaced as-is so
        the caller/retry decides.
        """
        try:
            await self._call_no_retry(self._client.head_bucket, Bucket=self._bucket)
        except TransientUpstreamError:
            raise
        except ClientError as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
            if status == 404:
                return False
            raise classify_botocore_error(exc) from exc
        return True

    async def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        """Upload bytes and return the durable ref ``s3://{bucket}/{key}``."""
        await self._call(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return f"s3://{self._bucket}/{key}"

    async def get_bytes(self, key: str) -> bytes:
        """Download and return the raw bytes stored under ``key``.

        The whole socket interaction (get_object + body read) happens inside one
        offloaded closure — see ``_read_object_bytes``.
        """
        return await self._read_object_bytes(key)

    # ---- internals ---------------------------------------------------------

    async def _read_object_bytes(self, key: str) -> bytes:
        """Fetch the object and read its body fully inside ONE offloaded call.

        get_object returns a streaming body whose ``.read()`` hits the network,
        so the read must not happen on the loop. We offload a closure that does
        both get_object and ``.read()`` so the whole socket interaction is
        off-loop.
        """

        def _fetch_and_read() -> bytes:
            obj = self._client.get_object(Bucket=self._bucket, Key=key)
            body = obj["Body"]
            try:
                return body.read()
            finally:
                body.close()

        return await self._call(_fetch_and_read)

    # ---- offload + retry plumbing -----------------------------------------

    async def _call[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run a blocking boto3 op with retry-over-offload + error translation.

        Retry is the OUTER loop; ``offloader.run`` the INNER action. Only
        ``TransientUpstreamError`` is retried (the retry policy's predicate);
        ``PermanentUpstreamError`` propagates on the first attempt.
        """
        async for attempt in retrying(self._retry):
            with attempt:
                try:
                    return await self._offload.run(fn, *args, **kwargs)
                except Exception as exc:
                    raise classify_botocore_error(exc) from exc
        raise AssertionError("retrying() always yields or raises")  # pragma: no cover

    async def _call_no_retry[**P, R](
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Single offloaded op without retry (used by the existence probe).

        A raw ``ClientError`` is re-raised unchanged so ``bucket_exists`` can
        inspect the HTTP status (the 404 path); everything else is classified.
        """
        try:
            return await self._offload.run(fn, *args, **kwargs)
        except ClientError:
            raise  # let caller inspect the HTTP status (404 path)
        except Exception as exc:
            raise classify_botocore_error(exc) from exc
