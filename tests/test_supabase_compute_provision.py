from __future__ import annotations

import json
from pathlib import Path

import pytest

from metaflow_serverless.providers.base import DatabaseCredentials
from metaflow_serverless.providers.supabase import SupabaseComputeProvider


@pytest.mark.asyncio
async def test_supabase_compute_provision_sets_quota_secrets(monkeypatch, tmp_path):
    provider = SupabaseComputeProvider()

    async def fake_ensure_cli_installed():
        return None

    monkeypatch.setattr(provider, "ensure_cli_installed", fake_ensure_cli_installed)
    monkeypatch.setattr(provider, "_find_edge_function_dir", lambda: tmp_path / "edge_function")
    (tmp_path / "edge_function").mkdir()
    (tmp_path / "edge_function" / "index.ts").write_text("// test\n", encoding="utf-8")
    (tmp_path / "edge_function" / "deno.json").write_text("{}", encoding="utf-8")

    monkeypatch.setenv("MF_MONTHLY_REQUEST_LIMIT", "12345")
    monkeypatch.setenv("MF_MONTHLY_EGRESS_LIMIT_BYTES", "67890")
    monkeypatch.setenv("MF_QUOTA_SCOPE", "test-scope")

    captured_env = {}

    async def fake_run_async(cmd, **kwargs):
        if cmd[:3] == ["supabase", "projects", "list"]:
            return (
                0,
                json.dumps([{"id": "proj_123", "name": "metaflow"}]),
                "",
            )
        if cmd[:3] == ["supabase", "secrets", "set"]:
            env_path = cmd[cmd.index("--env-file") + 1]
            captured_env["text"] = Path(env_path).read_text(encoding="utf-8")
            return (0, "", "")
        if cmd[:3] == ["supabase", "functions", "deploy"]:
            return (0, "", "")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("metaflow_serverless.providers.supabase._run_async", fake_run_async)

    db = DatabaseCredentials(
        dsn="postgresql://user:pass@host:5432/db",
        host="host",
        port=5432,
        database="db",
        username="user",
        password="pass",
    )

    creds = await provider.provision(db, "metaflow")

    assert creds.service_url == "https://proj_123.supabase.co/functions/v1/metadata-router"
    assert "MF_MONTHLY_REQUEST_LIMIT=12345" in captured_env["text"]
    assert "MF_MONTHLY_EGRESS_LIMIT_BYTES=67890" in captured_env["text"]
    assert "MF_QUOTA_SCOPE=test-scope" in captured_env["text"]
