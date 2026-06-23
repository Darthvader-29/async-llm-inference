"""Unit tests for Settings — env parsing, nesting, and the zero-cloud redirect.

These are pure unit tests: no network, no Docker, no event loop. We pass env
via monkeypatch and construct Settings(_env_file=None) so a developer's local
.env cannot influence the result.
"""

from __future__ import annotations

import pytest

from app.core.config import Environment, ObjectStoreSettings, Settings

# Tests set env via monkeypatch.setenv (auto-reverted per test) and always
# construct Settings(_env_file=None) so a developer's local .env can never
# influence the result. There is no shared builder helper — each test makes the
# exact Settings it needs, which keeps the cause of any failure obvious.


# --- Defaults & the zero-cloud redirect ------------------------------------
@pytest.mark.parametrize(
    ("env_value", "expect_endpoint", "expect_path_style"),
    [
        (Environment.DEV, "http://localhost:9000", True),  # dev → MinIO
        (Environment.TEST, "http://localhost:9000", True),  # test → MinIO
        (Environment.PROD, None, False),  # prod → untouched
    ],
)
def test_zero_cloud_redirect_matrix(
    env_value: Environment,
    expect_endpoint: str | None,
    expect_path_style: bool,
) -> None:
    """Non-prod with no endpoint forces MinIO + path-style; prod is left alone."""
    settings = Settings(env=env_value, _env_file=None)
    assert settings.object_store.endpoint_url == expect_endpoint
    assert settings.object_store.force_path_style is expect_path_style


def test_explicit_endpoint_is_respected_even_in_dev() -> None:
    """If an operator sets an explicit S3 endpoint, the redirect must NOT override it."""
    # Pass an explicit ObjectStoreSettings (not a dict): the pydantic mypy plugin
    # types the field as ObjectStoreSettings, so a dict literal would fail
    # mypy --strict with [arg-type]. The explicit model is type-correct and has
    # identical runtime behavior.
    settings = Settings(
        env=Environment.DEV,
        object_store=ObjectStoreSettings(endpoint_url="https://s3.eu-west-1.amazonaws.com"),
        _env_file=None,
    )
    assert settings.object_store.endpoint_url == "https://s3.eu-west-1.amazonaws.com"
    # force_path_style stays at its default (False) because the branch didn't run.
    assert settings.object_store.force_path_style is False


# --- Env parsing & prefixing -----------------------------------------------
def test_env_prefix_and_flat_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """AIE_-prefixed flat env vars populate top-level fields."""
    monkeypatch.setenv("AIE_ENV", "prod")
    monkeypatch.setenv("AIE_DATABASE_URL", "postgresql+asyncpg://u:p@db:5432/x")
    monkeypatch.setenv("AIE_OFFLOAD_MAX_WORKERS", "64")
    settings = Settings(_env_file=None)
    assert settings.env is Environment.PROD
    assert settings.is_prod is True
    assert settings.database_url == "postgresql+asyncpg://u:p@db:5432/x"
    assert settings.offload_max_workers == 64


def test_nested_delimiter_populates_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    """AIE_<GROUP>__<FIELD> populates nested settings groups via '__'."""
    monkeypatch.setenv("AIE_BROKER__WORKER_CONCURRENCY", "16")
    monkeypatch.setenv("AIE_BROKER__MAX_ATTEMPTS", "5")
    monkeypatch.setenv("AIE_RETRY__BASE_DELAY_S", "0")
    monkeypatch.setenv("AIE_OBJECT_STORE__BUCKET", "custom-bucket")
    settings = Settings(_env_file=None)
    assert settings.broker.worker_concurrency == 16
    assert settings.broker.max_attempts == 5
    assert settings.retry.base_delay_s == 0.0  # tests set this to count attempts
    assert settings.object_store.bucket == "custom-bucket"


def test_api_keys_parsed_into_frozenset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Comma-separated AIE_API_KEYS parses into an immutable, deduplicated set."""
    monkeypatch.setenv("AIE_API_KEYS", "k1,k2,k2,k3")
    settings = Settings(_env_file=None)
    assert settings.api_keys == frozenset({"k1", "k2", "k3"})
    assert isinstance(settings.api_keys, frozenset)


def test_provider_secrets_are_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    """SecretStr secrets never appear in repr()/str() of the value."""
    monkeypatch.setenv("AIE_HUGGINGFACE_TOKEN", "hf_supersecret")
    settings = Settings(_env_file=None)
    assert settings.huggingface_token is not None
    # The secret value is retrievable explicitly but hidden in repr.
    assert settings.huggingface_token.get_secret_value() == "hf_supersecret"
    assert "hf_supersecret" not in repr(settings.huggingface_token)


def test_unknown_env_vars_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """extra='ignore' lets unrelated process env vars coexist without error."""
    monkeypatch.setenv("AIE_TOTALLY_UNKNOWN", "whatever")
    # Should not raise despite the unknown AIE_-prefixed var.
    settings = Settings(_env_file=None)
    assert settings.env is Environment.DEV
