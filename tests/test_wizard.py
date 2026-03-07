"""
Tests for the setup wizard (metaflow_serverless.setup.wizard).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from metaflow_serverless.providers.base import (
    ComputeCredentials,
    DatabaseCredentials,
    StorageCredentials,
)
from metaflow_serverless.providers.registry import COMPATIBLE_STACKS, compatible_storage


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_db_creds(**kwargs) -> DatabaseCredentials:
    defaults = dict(
        dsn="postgresql://user:pass@host:5432/db",
        host="host",
        port=5432,
        database="db",
        username="user",
        password="pass",
    )
    defaults.update(kwargs)
    return DatabaseCredentials(**defaults)


def _make_storage_creds(**kwargs) -> StorageCredentials:
    defaults = dict(
        endpoint_url="https://bucket.example.com",
        access_key_id="AKID",
        secret_access_key="secret",
        bucket="testbucket",
        region="auto",
    )
    defaults.update(kwargs)
    return StorageCredentials(**defaults)


def _make_compute_creds(**kwargs) -> ComputeCredentials:
    defaults = dict(service_url="https://myservice.run.app")
    defaults.update(kwargs)
    return ComputeCredentials(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWizardWritesConfig:
    async def test_wizard_writes_config_after_provision(self, tmp_path):
        """
        When all providers successfully provision, _write_config is called
        with the METAFLOW_SERVICE_URL and storage keys from the credentials.
        """
        from metaflow_serverless.setup.wizard import SetupWizard

        cfg_path = tmp_path / ".metaflowconfig"
        wizard = SetupWizard(config_path=str(cfg_path))

        db_creds = _make_db_creds()
        storage_creds = _make_storage_creds()
        compute_creds = _make_compute_creds()

        # Call _write_config directly with test credentials.
        wizard._write_config(db_creds, storage_creds, compute_creds)

        # Verify the config file was written.
        assert cfg_path.exists()
        data = json.loads(cfg_path.read_text())

        assert data["METAFLOW_SERVICE_URL"] == compute_creds.service_url
        assert data["METAFLOW_S3_ENDPOINT_URL"] == storage_creds.endpoint_url
        assert data["AWS_ACCESS_KEY_ID"] == storage_creds.access_key_id
        assert data["AWS_SECRET_ACCESS_KEY"] == storage_creds.secret_access_key
        assert "metaflow" in data["METAFLOW_DATASTORE_SYSROOT_S3"]
        assert storage_creds.bucket in data["METAFLOW_DATASTORE_SYSROOT_S3"]

    async def test_wizard_write_config_sets_default_metadata(self, tmp_path):
        """_write_config sets default metadata and datastore settings."""
        from metaflow_serverless.setup.wizard import SetupWizard

        cfg_path = tmp_path / ".metaflowconfig"
        wizard = SetupWizard(config_path=str(cfg_path))
        wizard._write_config(_make_db_creds(), _make_storage_creds(), _make_compute_creds())
        data = json.loads(cfg_path.read_text())
        assert data.get("METAFLOW_DEFAULT_METADATA") == "service"
        assert data.get("METAFLOW_DEFAULT_DATASTORE") == "s3"

    async def test_wizard_write_config_sets_service_auth_key(self, tmp_path):
        """_write_config writes METAFLOW_SERVICE_AUTH_KEY when provided."""
        from metaflow_serverless.setup.wizard import SetupWizard

        cfg_path = tmp_path / ".metaflowconfig"
        wizard = SetupWizard(config_path=str(cfg_path))
        wizard._write_config(
            _make_db_creds(),
            _make_storage_creds(),
            _make_compute_creds(service_auth_key="svc-key-1"),
        )
        data = json.loads(cfg_path.read_text())
        assert data.get("METAFLOW_SERVICE_AUTH_KEY") == "svc-key-1"


class TestWizardMigrationsAsyncpg:
    async def test_wizard_runs_migrations_asyncpg(self, tmp_path):
        """
        _run_migrations_asyncpg is called with the db DSN.
        """
        from metaflow_serverless.setup.wizard import SetupWizard, _run_migrations_asyncpg

        cfg_path = tmp_path / ".metaflowconfig"
        wizard = SetupWizard(config_path=str(cfg_path))
        db_creds = _make_db_creds(dsn="postgresql://user:pass@host:5432/mydb")

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch(
            "metaflow_serverless.setup.wizard._run_migrations_asyncpg",
            new_callable=AsyncMock,
        ) as mock_migrate:
            await wizard._run_migrations("neon", db_creds, "testproject")
            mock_migrate.assert_awaited_once_with(db_creds.dsn)

    async def test_wizard_migrations_asyncpg_connects_and_executes(self, tmp_path):
        """_run_migrations_asyncpg calls asyncpg.connect and executes SQL."""
        from metaflow_serverless.setup.wizard import _run_migrations_asyncpg

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", return_value=mock_conn) as mock_connect:
            await _run_migrations_asyncpg("postgresql://user:pass@host:5432/db")

        mock_connect.assert_awaited_once_with("postgresql://user:pass@host:5432/db")
        # execute is called for schema, procedures, and PostgREST schema reload.
        assert mock_conn.execute.await_count == 3
        mock_conn.close.assert_awaited_once()

    async def test_wizard_migrations_supabase_uses_asyncpg(self, tmp_path):
        """Supabase migrations also run through asyncpg."""
        from metaflow_serverless.setup.wizard import SetupWizard

        cfg_path = tmp_path / ".metaflowconfig"
        wizard = SetupWizard(config_path=str(cfg_path))
        db_creds = _make_db_creds()

        with patch(
            "metaflow_serverless.setup.wizard._run_migrations_asyncpg",
            new_callable=AsyncMock,
        ) as mock_asyncpg:
            await wizard._run_migrations("supabase", db_creds, "testproject")

        mock_asyncpg.assert_awaited_once_with(db_creds.dsn)


class TestCompatibleStorageFiltered:
    def test_wizard_compatible_storage_filtered_cloud_run(self):
        """
        For cloud-run compute, storage choices do NOT include 'supabase'.
        """
        storage_options = compatible_storage("cloud-run")
        assert "supabase" not in storage_options
        assert "r2" in storage_options
        assert "b2" in storage_options

    def test_wizard_compatible_storage_filtered_supabase(self):
        """
        For supabase compute, all storage options are available.
        """
        storage_options = compatible_storage("supabase")
        assert "supabase" in storage_options
        assert "r2" in storage_options
        assert "b2" in storage_options

    def test_wizard_compatible_storage_filtered_render(self):
        """
        For render compute, storage choices do NOT include 'supabase'.
        """
        storage_options = compatible_storage("render")
        assert "supabase" not in storage_options
        assert "r2" in storage_options
        assert "b2" in storage_options

    def test_wizard_storage_registry_filtered_by_compute(self):
        """
        The wizard filters STORAGE_PROVIDERS using compatible_storage.
        Verify that the filtering logic works as expected.
        """
        from metaflow_serverless.providers.registry import STORAGE_PROVIDERS

        compute_name = "cloud-run"
        compat_storage_names = compatible_storage(compute_name)
        filtered = {k: v for k, v in STORAGE_PROVIDERS.items() if k in compat_storage_names}

        assert "supabase" not in filtered
        assert "r2" in filtered
        assert "b2" in filtered
