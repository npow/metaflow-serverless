"""
Provider package: exports all provider classes and credential dataclasses.
"""

from .backblaze_b2 import BackblazeB2Provider
from .base import (
    ComputeCredentials,
    ComputeProvider,
    DatabaseCredentials,
    DatabaseProvider,
    StorageCredentials,
    StorageProvider,
)
from .cloud_run import CloudRunProvider
from .cockroachdb import CockroachDBProvider
from .cloudflare_r2 import CloudflareR2Provider
from .neon import NeonProvider
from .registry import (
    COMPATIBLE_STACKS,
    COMPUTE_PROVIDERS,
    DATABASE_PROVIDERS,
    STORAGE_PROVIDERS,
    compatible_databases,
    compatible_storage,
    get_compute_provider,
    get_database_provider,
    get_storage_provider,
)
from .render import RenderProvider
from .supabase import (
    SupabaseComputeProvider,
    SupabaseDatabaseProvider,
    SupabaseStorageProvider,
)

__all__ = [
    # Base classes and credential types
    "DatabaseProvider",
    "StorageProvider",
    "ComputeProvider",
    "DatabaseCredentials",
    "StorageCredentials",
    "ComputeCredentials",
    # Concrete implementations — database
    "SupabaseDatabaseProvider",
    "NeonProvider",
    "CockroachDBProvider",
    # Concrete implementations — storage
    "SupabaseStorageProvider",
    "CloudflareR2Provider",
    "BackblazeB2Provider",
    # Concrete implementations — compute
    "SupabaseComputeProvider",
    "CloudRunProvider",
    "RenderProvider",
    # Registry helpers
    "DATABASE_PROVIDERS",
    "STORAGE_PROVIDERS",
    "COMPUTE_PROVIDERS",
    "COMPATIBLE_STACKS",
    "get_database_provider",
    "get_storage_provider",
    "get_compute_provider",
    "compatible_databases",
    "compatible_storage",
]
