"""
Tests for provider base attributes and credential dataclasses.

These tests only check metadata (name, display_name, requires_cc, verification)
without making any network or CLI calls.
"""
from __future__ import annotations

import pytest

from metaflow_ephemeral.providers.base import (
    DatabaseCredentials,
    StorageCredentials,
    ComputeCredentials,
    DatabaseProvider,
    StorageProvider,
    ComputeProvider,
)
from metaflow_ephemeral.providers.supabase import (
    SupabaseDatabaseProvider,
    SupabaseStorageProvider,
    SupabaseComputeProvider,
)
from metaflow_ephemeral.providers.neon import NeonProvider
from metaflow_ephemeral.providers.cockroachdb import CockroachDBProvider
from metaflow_ephemeral.providers.cloudflare_r2 import CloudflareR2Provider
from metaflow_ephemeral.providers.backblaze_b2 import BackblazeB2Provider
from metaflow_ephemeral.providers.cloud_run import CloudRunProvider
from metaflow_ephemeral.providers.render import RenderProvider


# ---------------------------------------------------------------------------
# Helper to check provider attributes have correct types
# ---------------------------------------------------------------------------

def _check_provider_attrs(provider_instance):
    """Assert that a provider has all required typed attributes."""
    assert isinstance(provider_instance.name, str), "name must be str"
    assert isinstance(provider_instance.display_name, str), "display_name must be str"
    assert isinstance(provider_instance.requires_cc, bool), "requires_cc must be bool"
    assert isinstance(provider_instance.verification, str), "verification must be str"
    # cli_name may be str or None
    assert provider_instance.cli_name is None or isinstance(
        provider_instance.cli_name, str
    ), "cli_name must be str or None"


# ---------------------------------------------------------------------------
# Database providers
# ---------------------------------------------------------------------------

class TestSupabaseDatabaseProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = SupabaseDatabaseProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert SupabaseDatabaseProvider.name == "supabase"

    def test_supabase_db_no_cc(self):
        """SupabaseDatabaseProvider.requires_cc is False (free tier, no CC)."""
        assert SupabaseDatabaseProvider.requires_cc is False

    def test_verification_email(self):
        assert SupabaseDatabaseProvider.verification == "email"


class TestNeonProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = NeonProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert NeonProvider.name == "neon"

    def test_neon_no_cc(self):
        """NeonProvider.requires_cc is False (free tier, no CC)."""
        assert NeonProvider.requires_cc is False

    def test_cli_name(self):
        assert NeonProvider.cli_name == "neonctl"


class TestCockroachDBProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = CockroachDBProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert CockroachDBProvider.name == "cockroachdb"

    def test_cockroachdb_no_cc(self):
        """CockroachDBProvider.requires_cc is False (free tier, no CC)."""
        assert CockroachDBProvider.requires_cc is False

    def test_cli_name(self):
        assert CockroachDBProvider.cli_name == "ccloud"


# ---------------------------------------------------------------------------
# Storage providers
# ---------------------------------------------------------------------------

class TestSupabaseStorageProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = SupabaseStorageProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert SupabaseStorageProvider.name == "supabase"

    def test_no_cc(self):
        assert SupabaseStorageProvider.requires_cc is False


class TestCloudflareR2Provider:
    def test_attributes_exist_and_have_correct_types(self):
        p = CloudflareR2Provider()
        _check_provider_attrs(p)

    def test_name(self):
        assert CloudflareR2Provider.name == "r2"

    def test_cloudflare_r2_requires_cc(self):
        """CloudflareR2Provider.requires_cc is True (R2 needs CC even on free tier)."""
        assert CloudflareR2Provider.requires_cc is True

    def test_cli_name(self):
        assert CloudflareR2Provider.cli_name == "wrangler"


class TestBackblazeB2Provider:
    def test_attributes_exist_and_have_correct_types(self):
        p = BackblazeB2Provider()
        _check_provider_attrs(p)

    def test_name(self):
        assert BackblazeB2Provider.name == "b2"

    def test_no_cc(self):
        assert BackblazeB2Provider.requires_cc is False

    def test_backblaze_phone_verification(self):
        """BackblazeB2Provider.verification == 'phone' (requires phone verification)."""
        assert BackblazeB2Provider.verification == "phone"

    def test_no_cli(self):
        """BackblazeB2Provider uses REST API directly; cli_name is None."""
        assert BackblazeB2Provider.cli_name is None


# ---------------------------------------------------------------------------
# Compute providers
# ---------------------------------------------------------------------------

class TestSupabaseComputeProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = SupabaseComputeProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert SupabaseComputeProvider.name == "supabase"

    def test_no_cc(self):
        assert SupabaseComputeProvider.requires_cc is False


class TestCloudRunProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = CloudRunProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert CloudRunProvider.name == "cloud-run"

    def test_cloud_run_requires_cc(self):
        """CloudRunProvider.requires_cc is True (GCP requires CC)."""
        assert CloudRunProvider.requires_cc is True

    def test_verification(self):
        assert CloudRunProvider.verification == "email+cc"

    def test_cli_name(self):
        assert CloudRunProvider.cli_name == "gcloud"


class TestRenderProvider:
    def test_attributes_exist_and_have_correct_types(self):
        p = RenderProvider()
        _check_provider_attrs(p)

    def test_name(self):
        assert RenderProvider.name == "render"

    def test_render_requires_cc(self):
        """RenderProvider.requires_cc is True (Render requires CC for free tier)."""
        assert RenderProvider.requires_cc is True

    def test_no_cli(self):
        """RenderProvider uses REST API directly; cli_name is None."""
        assert RenderProvider.cli_name is None


# ---------------------------------------------------------------------------
# Credential dataclasses
# ---------------------------------------------------------------------------

class TestDatabaseCredentials:
    def test_credentials_repr_masks_password(self):
        """DatabaseCredentials repr does not expose the raw password."""
        creds = DatabaseCredentials(
            dsn="postgresql://user:supersecret@host:5432/db",
            host="host",
            port=5432,
            database="db",
            username="user",
            password="supersecret",
        )
        r = repr(creds)
        assert "supersecret" not in r
        assert "***" in r

    def test_credentials_repr_shows_host(self):
        """DatabaseCredentials repr includes the host."""
        creds = DatabaseCredentials(
            dsn="postgresql://user:pass@myhost:5432/mydb",
            host="myhost",
            port=5432,
            database="mydb",
            username="user",
            password="pass",
        )
        assert "myhost" in repr(creds)

    def test_credentials_repr_empty_password(self):
        """DatabaseCredentials repr doesn't crash when password is empty."""
        creds = DatabaseCredentials(
            dsn="postgresql://user@host:5432/db",
            host="host",
            port=5432,
            database="db",
            username="user",
            password="",
        )
        r = repr(creds)
        assert "host" in r


class TestStorageCredentials:
    def test_repr_shows_endpoint(self):
        creds = StorageCredentials(
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="AKID",
            secret_access_key="secret",
            bucket="mybucket",
            region="auto",
        )
        assert "example.r2.cloudflarestorage.com" in repr(creds)

    def test_repr_does_not_expose_secret(self):
        """StorageCredentials repr does not include the secret_access_key."""
        creds = StorageCredentials(
            endpoint_url="https://ep.example.com",
            access_key_id="AKID",
            secret_access_key="topsecretkey",
            bucket="bucket",
            region="us-east-1",
        )
        assert "topsecretkey" not in repr(creds)


class TestComputeCredentials:
    def test_repr_shows_url(self):
        creds = ComputeCredentials(service_url="https://myservice.run.app")
        assert "myservice.run.app" in repr(creds)
