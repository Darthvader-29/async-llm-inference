"""Publish-retry is clock-free: attempts are COUNTED, never timed.

``fail_times`` drives how many transient publish failures occur; ``max_delay_s=0``
(via ``fake_settings``) caps every backoff to zero so the loop never sleeps.
"""

from __future__ import annotations

import pytest

from app.domain.exceptions import TransientUpstreamError
from app.domain.models import JobType
from app.services.ingestion import IngestionService
from tests.support.container import fake_settings
from tests.support.fakes import FakeQueue, InMemoryRepository


async def test_publish_retries_then_succeeds_without_sleeping() -> None:
    settings = fake_settings()  # max_attempts=3, base/max delay 0
    repo = InMemoryRepository()
    queue = FakeQueue(fail_times=2)  # fail twice, succeed on the 3rd attempt
    svc = IngestionService(repo, queue, settings.retry)

    job_id = await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    # Row persisted once; publish ultimately succeeded after 2 transient fails.
    assert job_id in repo.store
    assert len(queue.published) == 1


async def test_publish_exhausts_attempts_and_reraises() -> None:
    settings = fake_settings()  # max_attempts=3
    repo = InMemoryRepository()
    queue = FakeQueue(fail_times=99)  # always fails
    svc = IngestionService(repo, queue, settings.retry)

    with pytest.raises(TransientUpstreamError):
        await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    # The row was still written PENDING before publish failed (source of truth).
    assert len(repo.store) == 1
    assert len(queue.published) == 0
