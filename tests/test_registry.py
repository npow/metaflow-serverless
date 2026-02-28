"""
Tests for metaflow_serverless.providers.registry.
"""
from __future__ import annotations

import pytest

from metaflow_serverless.providers.registry import (
    COMPATIBLE_STACKS,
    COMPUTE_PROVIDERS,
    DATABASE_PROVIDERS,
    STORAGE_PROVIDERS,
    get_database_provider,
    get_storage_provider,
    get_compute_provider,
    compatible_databases,
    compatible_storage,
)


class TestDatabaseProviders:
    def test_all_database_providers_registered(self):
        """All 3 expected database providers are in the registry."""
        assert "supabase" in DATABASE_PROVIDERS
        assert "neon" in DATABASE_PROVIDERS
        assert "cockroachdb" in DATABASE_PROVIDERS

    def test_database_provider_count(self):
        """Exactly 3 database providers are registered."""
        assert len(DATABASE_PROVIDERS) == 3


class TestStorageProviders:
    def test_all_storage_providers_registered(self):
        """All 3 expected storage providers are in the registry."""
        assert "supabase" in STORAGE_PROVIDERS
        assert "r2" in STORAGE_PROVIDERS
        assert "b2" in STORAGE_PROVIDERS

    def test_storage_provider_count(self):
        """Exactly 3 storage providers are registered."""
        assert len(STORAGE_PROVIDERS) == 3


class TestComputeProviders:
    def test_all_compute_providers_registered(self):
        """All 3 expected compute providers are in the registry."""
        assert "supabase" in COMPUTE_PROVIDERS
        assert "cloud-run" in COMPUTE_PROVIDERS
        assert "render" in COMPUTE_PROVIDERS

    def test_compute_provider_count(self):
        """Exactly 3 compute providers are registered."""
        assert len(COMPUTE_PROVIDERS) == 3


class TestGetProviders:
    def test_get_database_provider_returns_instance(self):
        """get_database_provider returns an instantiated provider."""
        provider = get_database_provider("neon")
        assert provider is not None
        assert provider.name == "neon"

    def test_get_storage_provider_returns_instance(self):
        """get_storage_provider returns an instantiated provider."""
        provider = get_storage_provider("r2")
        assert provider is not None
        assert provider.name == "r2"

    def test_get_compute_provider_returns_instance(self):
        """get_compute_provider returns an instantiated provider."""
        provider = get_compute_provider("render")
        assert provider is not None
        assert provider.name == "render"

    def test_get_database_provider_unknown(self):
        """get_database_provider raises ValueError for unknown providers."""
        with pytest.raises(ValueError, match="Unknown database provider"):
            get_database_provider("nonexistent")

    def test_get_storage_provider_unknown(self):
        """get_storage_provider raises ValueError for unknown providers."""
        with pytest.raises(ValueError):
            get_storage_provider("nonexistent")

    def test_get_compute_provider_unknown(self):
        """get_compute_provider raises ValueError for unknown providers."""
        with pytest.raises(ValueError):
            get_compute_provider("nonexistent")


class TestCompatibleStacks:
    def test_compatible_stacks_structure(self):
        """COMPATIBLE_STACKS has the expected top-level keys."""
        assert "supabase" in COMPATIBLE_STACKS
        assert "cloud-run" in COMPATIBLE_STACKS
        assert "render" in COMPATIBLE_STACKS

    def test_compatible_stacks_have_database_and_storage(self):
        """Each stack entry has 'database' and 'storage' sub-keys."""
        for compute, compat in COMPATIBLE_STACKS.items():
            assert "database" in compat, f"{compute} missing 'database' key"
            assert "storage" in compat, f"{compute} missing 'storage' key"

    def test_compatible_stacks_values_are_lists(self):
        """The database and storage values are non-empty lists."""
        for compute, compat in COMPATIBLE_STACKS.items():
            assert isinstance(compat["database"], list), f"{compute}: database must be list"
            assert isinstance(compat["storage"], list), f"{compute}: storage must be list"
            assert len(compat["database"]) > 0, f"{compute}: database list is empty"
            assert len(compat["storage"]) > 0, f"{compute}: storage list is empty"

    def test_supabase_compatible_with_all_databases(self):
        """Supabase compute accepts all 3 database options."""
        supabase_dbs = COMPATIBLE_STACKS["supabase"]["database"]
        assert "supabase" in supabase_dbs
        assert "neon" in supabase_dbs
        assert "cockroachdb" in supabase_dbs

    def test_cloud_run_not_compatible_with_supabase_db(self):
        """cloud-run compute does not list supabase as a compatible database."""
        assert "supabase" not in COMPATIBLE_STACKS["cloud-run"]["database"]

    def test_cloud_run_not_compatible_with_supabase_storage(self):
        """cloud-run compute does not list supabase as compatible storage."""
        assert "supabase" not in COMPATIBLE_STACKS["cloud-run"]["storage"]

    def test_compatible_databases_helper(self):
        """compatible_databases() returns the correct list for a compute provider."""
        dbs = compatible_databases("supabase")
        assert "neon" in dbs
        assert "cockroachdb" in dbs

    def test_compatible_storage_helper(self):
        """compatible_storage() returns the correct list for a compute provider."""
        storage = compatible_storage("cloud-run")
        assert "r2" in storage
        assert "b2" in storage

    def test_compatible_databases_unknown_compute(self):
        """compatible_databases() returns [] for unknown compute."""
        assert compatible_databases("nonexistent") == []

    def test_compatible_storage_unknown_compute(self):
        """compatible_storage() returns [] for unknown compute."""
        assert compatible_storage("nonexistent") == []
