from __future__ import annotations

import json

import pytest

from metaflow_serverless.providers.supabase import SupabaseStorageProvider


@pytest.mark.asyncio
async def test_supabase_storage_provision_uses_api_bucket_create(monkeypatch):
    provider = SupabaseStorageProvider(project_ref="proj_123")

    async def fake_ensure_cli_installed():
        return None

    monkeypatch.setattr(provider, "ensure_cli_installed", fake_ensure_cli_installed)

    keys_payload = [
        {"name": "anon", "api_key": "anon_key"},
        {"name": "service_role", "api_key": "service_key"},
    ]

    seen_cmds: list[list[str]] = []

    async def fake_run_async(cmd, **kwargs):
        seen_cmds.append(cmd)
        if cmd[:3] == ["supabase", "projects", "api-keys"]:
            return (0, json.dumps(keys_payload), "")
        raise AssertionError(f"Unexpected command: {cmd}")

    created: list[tuple[str, str, str]] = []

    async def fake_create_storage_bucket(project_ref, bucket_name, service_key):
        created.append((project_ref, bucket_name, service_key))

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._run_async",
        fake_run_async,
    )
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        fake_create_storage_bucket,
    )

    creds = await provider.provision("metaflow-metaflow")

    assert creds.bucket == "metaflow-metaflow"
    assert creds.access_key_id == "anon_key"
    assert creds.secret_access_key == "service_key"
    assert created == [("proj_123", "metaflow-metaflow", "service_key")]
    assert seen_cmds == [
        [
            "supabase",
            "projects",
            "api-keys",
            "--project-ref",
            "proj_123",
            "--output",
            "json",
        ]
    ]


@pytest.mark.asyncio
async def test_supabase_storage_provision_prefers_env_s3_keys(monkeypatch):
    provider = SupabaseStorageProvider(project_ref="proj_123")

    async def fake_ensure_cli_installed():
        return None

    monkeypatch.setattr(provider, "ensure_cli_installed", fake_ensure_cli_installed)
    monkeypatch.setenv("SUPABASE_S3_ACCESS_KEY_ID", "env_ak")
    monkeypatch.setenv("SUPABASE_S3_SECRET_ACCESS_KEY", "env_sk")

    keys_payload = [
        {"name": "anon", "api_key": "anon_key"},
        {"name": "service_role", "api_key": "service_key"},
    ]

    async def fake_run_async(cmd, **kwargs):
        if cmd[:3] == ["supabase", "projects", "api-keys"]:
            return (0, json.dumps(keys_payload), "")
        raise AssertionError(f"Unexpected command: {cmd}")

    async def fake_create_storage_bucket(project_ref, bucket_name, service_key):
        assert service_key == "service_key"

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._run_async",
        fake_run_async,
    )
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._create_storage_bucket",
        fake_create_storage_bucket,
    )

    creds = await provider.provision("metaflow-metaflow")
    assert creds.access_key_id == "env_ak"
    assert creds.secret_access_key == "env_sk"
