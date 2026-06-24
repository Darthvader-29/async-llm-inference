"""Object-store adapters (Phase 4).

Re-exports the concrete ``S3ObjectStore`` so callers can import it from the
package root. Error classification lives in ``errors`` and stays import-light.
"""

from __future__ import annotations

from app.adapters.object_store.s3 import S3ObjectStore

__all__ = ["S3ObjectStore"]
