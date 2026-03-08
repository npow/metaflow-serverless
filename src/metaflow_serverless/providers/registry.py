"""
Provider registry: maps provider name strings to their implementation classes.

Import this module to discover which providers are available and which stack
combinations are compatible.
"""

from __future__ import annotations

from .backblaze_b2 import BackblazeB2Provider
from .base import ComputeProvider, DatabaseProvider, StorageProvider
from .cloud_run import CloudRunProvider
from .cloudflare_r2 import CloudflareR2Provider
from .cockroachdb import CockroachDBProvider
from .neon import NeonProvider
from .render import RenderProvider
from .supabase import (
    SupabaseComputeProvider,
    SupabaseDatabaseProvider,
    SupabaseStorageProvider,
)

# ---------------------------------------------------------------------------
# Provider registries
# ---------------------------------------------------------------------------

DATABASE_PROVIDERS: dict[str, type[DatabaseProvider]] = {
    "supabase": SupabaseDatabaseProvider,
    "neon": NeonProvider,
    "cockroachdb": CockroachDBProvider,
}

STORAGE_PROVIDERS: dict[str, type[StorageProvider]] = {
    "supabase": SupabaseStorageProvider,
    "r2": CloudflareR2Provider,
    "b2": BackblazeB2Provider,
}

COMPUTE_PROVIDERS: dict[str, type[ComputeProvider]] = {
    "supabase": SupabaseComputeProvider,
    "cloud-run": CloudRunProvider,
    "render": RenderProvider,
}

# ---------------------------------------------------------------------------
# Compatible stack definitions
# ---------------------------------------------------------------------------

# Maps each compute provider to the database and storage providers that can
# be combined with it.  This reflects both technical compatibility (e.g.
# Cloud Run cannot use Supabase Storage because it needs an external S3
# endpoint) and practical constraints (e.g. Supabase natively integrates its
# own Postgres, but also works with Neon or CockroachDB via connection string).
COMPATIBLE_STACKS: dict[str, dict[str, list[str]]] = {
    "supabase": {
        "database": ["supabase", "neon", "cockroachdb"],
        "storage": ["supabase", "r2", "b2"],
    },
    "cloud-run": {
        "database": ["neon", "cockroachdb"],
        "storage": ["r2", "b2"],
    },
    "render": {
        "database": ["neon", "cockroachdb"],
        "storage": ["r2", "b2"],
    },
}


def get_database_provider(name: str) -> DatabaseProvider:
    """
    Instantiate and return a DatabaseProvider by name.

    Raises
    ------
    ValueError
        If no database provider with the given *name* is registered.
    """
    cls = DATABASE_PROVIDERS.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown database provider: {name!r}. Available: {sorted(DATABASE_PROVIDERS.keys())}"
        )
    return cls()


def get_storage_provider(name: str) -> StorageProvider:
    """
    Instantiate and return a StorageProvider by name.

    Raises
    ------
    ValueError
        If no storage provider with the given *name* is registered.
    """
    cls = STORAGE_PROVIDERS.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown storage provider: {name!r}. Available: {sorted(STORAGE_PROVIDERS.keys())}"
        )
    return cls()


def get_compute_provider(name: str) -> ComputeProvider:
    """
    Instantiate and return a ComputeProvider by name.

    Raises
    ------
    ValueError
        If no compute provider with the given *name* is registered.
    """
    cls = COMPUTE_PROVIDERS.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown compute provider: {name!r}. Available: {sorted(COMPUTE_PROVIDERS.keys())}"
        )
    return cls()


def compatible_databases(compute: str) -> list[str]:
    """Return the list of database provider names compatible with *compute*."""
    return COMPATIBLE_STACKS.get(compute, {}).get("database", [])


def compatible_storage(compute: str) -> list[str]:
    """Return the list of storage provider names compatible with *compute*."""
    return COMPATIBLE_STACKS.get(compute, {}).get("storage", [])
