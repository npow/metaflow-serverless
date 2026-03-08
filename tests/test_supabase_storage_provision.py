from __future__ import annotations

import json

import pytest

from metaflow_serverless.providers.supabase import SupabaseStorageProvider

_KEYS_PAYLOAD = [
    {"name": "anon", "api_key": "anon_key"},
    {"name": "service_role", "api_key": "service_key"},
]


async def _fake_run_async_keys(cmd, **kwargs):
    if cmd[:3] == ["supabase", "projects", "api-keys"]:
        return (0, json.dumps(_KEYS_PAYLOAD), "")
    raise AssertionError(f"Unexpected command: {cmd}")


async def _noop_ensure_cli_installed():
    return None


async def _noop_create_storage_bucket(*_args, **_kw):
    return None


@pytest.mark.asyncio
async def test_supabase_storage_provision_generates_random_s3_keys(monkeypatch):
    """provision() generates a random S3 key pair, not the JWT API tokens."""
    provider = SupabaseStorageProvider(project_ref="proj_123")
    monkeypatch.setattr(provider, "ensure_cli_installed", _noop_ensure_cli_installed)
    monkeypatch.setattr("metaflow_serverless.providers.supabase._run_async", _fake_run_async_keys)
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        _noop_create_storage_bucket,
    )

    creds = await provider.provision("metaflow-metaflow")

    assert creds.bucket == "metaflow-metaflow"
    # Keys must NOT be the JWT tokens.
    assert creds.access_key_id != "anon_key"
    assert creds.secret_access_key != "service_key"
    # Keys must be non-empty strings.
    assert creds.access_key_id and isinstance(creds.access_key_id, str)
    assert creds.secret_access_key and isinstance(creds.secret_access_key, str)


@pytest.mark.asyncio
async def test_supabase_storage_provision_creates_bucket_with_service_role(monkeypatch):
    """The storage bucket is created with the service_role key (REST auth)."""
    provider = SupabaseStorageProvider(project_ref="proj_123")
    monkeypatch.setattr(provider, "ensure_cli_installed", _noop_ensure_cli_installed)
    monkeypatch.setattr("metaflow_serverless.providers.supabase._run_async", _fake_run_async_keys)

    captured: list[tuple] = []

    async def fake_create_bucket(project_ref, bucket_name, service_key):
        captured.append((project_ref, bucket_name, service_key))

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        fake_create_bucket,
    )

    await provider.provision("metaflow-metaflow")

    assert len(captured) == 1
    assert captured[0][0] == "proj_123"
    assert captured[0][1] == "metaflow-metaflow"
    assert captured[0][2] == "service_key"  # service_role JWT used for bucket REST API


@pytest.mark.asyncio
async def test_supabase_storage_provision_registers_s3_creds_in_db(monkeypatch):
    """When set_db_dsn() is called, credentials are inserted into storage.s3_credentials."""
    provider = SupabaseStorageProvider(project_ref="proj_123")
    provider.set_db_dsn("postgresql://user:pass@host/db")
    monkeypatch.setattr(provider, "ensure_cli_installed", _noop_ensure_cli_installed)
    monkeypatch.setattr("metaflow_serverless.providers.supabase._run_async", _fake_run_async_keys)
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        _noop_create_storage_bucket,
    )

    registered_calls: list[tuple] = []

    async def fake_register(dsn, access_key_id, secret_access_key, description="metaflow"):
        registered_calls.append((dsn, access_key_id, secret_access_key))
        return True

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._register_s3_credentials_in_db",
        fake_register,
    )

    creds = await provider.provision("metaflow-metaflow")

    assert len(registered_calls) == 1
    dsn, ak, sk = registered_calls[0]
    assert dsn == "postgresql://user:pass@host/db"
    assert ak == creds.access_key_id
    assert sk == creds.secret_access_key


@pytest.mark.asyncio
async def test_supabase_storage_provision_warns_when_db_unavailable(monkeypatch, capsys):
    """Without a DB DSN, provision() warns but still returns credentials."""
    provider = SupabaseStorageProvider(project_ref="proj_123")
    # No set_db_dsn() call.
    monkeypatch.setattr(provider, "ensure_cli_installed", _noop_ensure_cli_installed)
    monkeypatch.setattr("metaflow_serverless.providers.supabase._run_async", _fake_run_async_keys)
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        _noop_create_storage_bucket,
    )

    creds = await provider.provision("metaflow-metaflow")

    # Should still return valid-looking credentials.
    assert creds.bucket == "metaflow-metaflow"
    assert creds.access_key_id
    assert creds.secret_access_key
