from __future__ import annotations

import pytest

from metaflow_serverless.providers import supabase as supabase_mod


@pytest.mark.asyncio
async def test_require_supabase_login_ok(monkeypatch):
    async def fake_run_async(cmd, **kwargs):
        return (0, "[]", "")

    monkeypatch.setattr(supabase_mod, "_run_async", fake_run_async)

    await supabase_mod._require_supabase_login()


@pytest.mark.asyncio
async def test_require_supabase_login_fails_with_guidance(monkeypatch):
    async def fake_run_async(cmd, **kwargs):
        return (1, "", "Access token not provided")

    monkeypatch.setattr(supabase_mod, "_run_async", fake_run_async)

    with pytest.raises(RuntimeError, match="Run `supabase login`"):
        await supabase_mod._require_supabase_login()
