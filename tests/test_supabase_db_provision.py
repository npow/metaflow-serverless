from __future__ import annotations

import json

import pytest

from metaflow_serverless.providers.supabase import SupabaseDatabaseProvider


@pytest.mark.asyncio
async def test_supabase_db_provision_new_project_builds_dsn(monkeypatch):
    provider = SupabaseDatabaseProvider()

    async def fake_ensure_cli_installed():
        return None

    monkeypatch.setattr(provider, "ensure_cli_installed", fake_ensure_cli_installed)
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._get_org_id",
        lambda: "org_123",
    )
    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._generate_password",
        lambda length=32: "pw_abc",
    )

    async def fake_wait_for_project(project_ref, timeout_seconds=120, poll_interval=5.0):
        return None

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._wait_for_project",
        fake_wait_for_project,
    )

    calls: list[list[str]] = []

    async def fake_run_async(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:3] == ["supabase", "projects", "list"]:
            if len(calls) == 1:
                return (0, "[]", "")
            return (
                0,
                json.dumps(
                    [
                        {
                            "id": "proj_123",
                            "name": "metaflow",
                            "database": {"host": "db.proj_123.supabase.co"},
                        }
                    ]
                ),
                "",
            )
        if cmd[:3] == ["supabase", "projects", "create"]:
            return (0, json.dumps({"id": "proj_123"}), "")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._run_async",
        fake_run_async,
    )

    creds = await provider.provision("metaflow")

    assert creds.host == "db.proj_123.supabase.co"
    assert creds.username == "postgres"
    assert creds.database == "postgres"
    assert "pw_abc" in creds.dsn
    assert "sslmode=require" in creds.dsn


@pytest.mark.asyncio
async def test_supabase_db_provision_existing_project_requires_password(monkeypatch):
    provider = SupabaseDatabaseProvider()

    async def fake_ensure_cli_installed():
        return None

    monkeypatch.setattr(provider, "ensure_cli_installed", fake_ensure_cli_installed)
    monkeypatch.delenv("SUPABASE_DB_PASSWORD", raising=False)

    async def fake_run_async(cmd, **kwargs):
        if cmd[:3] == ["supabase", "projects", "list"]:
            return (
                0,
                json.dumps([{"id": "proj_123", "name": "metaflow"}]),
                "",
            )
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(
        "metaflow_serverless.providers.supabase._run_async",
        fake_run_async,
    )

    with pytest.raises(RuntimeError, match="SUPABASE_DB_PASSWORD"):
        await provider.provision("metaflow")
