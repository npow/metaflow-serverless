"""
Abstract base classes for database, storage, and compute providers.

Each concrete provider implements these interfaces to supply credentials for
a specific backend (e.g. Neon for database, Cloudflare R2 for storage,
Google Cloud Run for compute).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Credential dataclasses
# ---------------------------------------------------------------------------


@dataclass
class DatabaseCredentials:
    """Credentials returned after a database is provisioned."""

    dsn: str          # Full connection string: postgresql://user:pass@host/db
    host: str
    port: int
    database: str
    username: str
    password: str

    def __repr__(self) -> str:
        # Avoid leaking the password in logs/reprs.
        masked_dsn = self.dsn.replace(self.password, "***") if self.password else self.dsn
        return (
            f"DatabaseCredentials("
            f"host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, username={self.username!r}, "
            f"dsn={masked_dsn!r})"
        )


@dataclass
class StorageCredentials:
    """Credentials returned after object storage is provisioned."""

    endpoint_url: str        # S3-compatible endpoint, e.g. https://...r2.cloudflarestorage.com
    access_key_id: str
    secret_access_key: str
    bucket: str
    region: str

    def __repr__(self) -> str:
        return (
            f"StorageCredentials("
            f"endpoint_url={self.endpoint_url!r}, "
            f"bucket={self.bucket!r}, "
            f"region={self.region!r}, "
            f"access_key_id={self.access_key_id!r})"
        )


@dataclass
class ComputeCredentials:
    """Credentials / connection info returned after compute is provisioned."""

    service_url: str   # The URL Metaflow client will call, e.g. https://...run.app
    service_auth_key: str | None = None

    def __repr__(self) -> str:
        has_key = bool(self.service_auth_key)
        return (
            f"ComputeCredentials(service_url={self.service_url!r}, "
            f"service_auth_key={'***' if has_key else None})"
        )


# ---------------------------------------------------------------------------
# Abstract provider base classes
# ---------------------------------------------------------------------------


class DatabaseProvider(ABC):
    """
    Abstract interface that every database provider must implement.

    Attributes
    ----------
    name:
        Machine-readable identifier used in config files and registry keys,
        e.g. "neon", "supabase", "cockroachdb".
    display_name:
        Human-readable name shown in the setup wizard, e.g. "Neon (serverless Postgres)".
    requires_cc:
        Whether the provider requires a credit card to sign up.
    verification:
        Signup verification method: "email", "phone", or "email+cc".
    cli_name:
        Name of the CLI binary this provider uses (e.g. "neonctl"), or None if
        no CLI is needed.
    """

    name: str
    display_name: str
    requires_cc: bool
    verification: str  # "email" | "phone" | "email+cc"
    cli_name: str | None  # e.g. "neonctl"

    @abstractmethod
    async def ensure_cli_installed(self) -> None:
        """
        Check whether the provider's CLI tool is installed and install it if not.

        Should be a no-op when the CLI is already present.  Should raise
        ``RuntimeError`` if installation fails and cannot continue.
        """
        ...

    @abstractmethod
    async def login(self) -> None:
        """
        Authenticate the user with the provider.

        This may open a browser window, prompt for an API token, or invoke
        the provider's CLI login flow.  Should raise ``RuntimeError`` on
        authentication failure.
        """
        ...

    @abstractmethod
    async def provision(self, project_name: str) -> DatabaseCredentials:
        """
        Create a new database (or reuse an existing one) for *project_name*.

        Parameters
        ----------
        project_name:
            A short identifier used to name the database / project on the
            provider, e.g. "my-metaflow-project".

        Returns
        -------
        DatabaseCredentials
            Connection credentials for the provisioned database.
        """
        ...


class StorageProvider(ABC):
    """
    Abstract interface that every object-storage provider must implement.

    Attributes
    ----------
    name:
        Machine-readable identifier, e.g. "r2", "b2", "supabase".
    display_name:
        Human-readable name shown in the setup wizard.
    requires_cc:
        Whether the provider requires a credit card to sign up.
    verification:
        Signup verification method: "email", "phone", or "email+cc".
    cli_name:
        Name of the CLI binary, or None if no CLI is needed.
    """

    name: str
    display_name: str
    requires_cc: bool
    verification: str
    cli_name: str | None

    @abstractmethod
    async def ensure_cli_installed(self) -> None:
        """
        Check whether the provider's CLI tool is installed and install it if not.

        Should be a no-op when the CLI is already present.  Should raise
        ``RuntimeError`` if installation fails and cannot continue.
        """
        ...

    @abstractmethod
    async def login(self) -> None:
        """
        Authenticate the user with the provider.

        Should raise ``RuntimeError`` on authentication failure.
        """
        ...

    @abstractmethod
    async def provision(self, bucket_name: str) -> StorageCredentials:
        """
        Create a new bucket (or reuse an existing one) named *bucket_name*.

        Parameters
        ----------
        bucket_name:
            Name of the S3-compatible bucket to create or reuse.

        Returns
        -------
        StorageCredentials
            S3-compatible credentials for the provisioned bucket.
        """
        ...


class ComputeProvider(ABC):
    """
    Abstract interface that every compute provider must implement.

    Compute providers deploy the Metaflow metadata service container to a
    serverless or managed compute platform and return the public service URL.

    Attributes
    ----------
    name:
        Machine-readable identifier, e.g. "cloud-run", "render", "supabase".
    display_name:
        Human-readable name shown in the setup wizard.
    requires_cc:
        Whether the provider requires a credit card to sign up.
    verification:
        Signup verification method: "email", "phone", or "email+cc".
    cli_name:
        Name of the CLI binary, or None if no CLI is needed.
    """

    name: str
    display_name: str
    requires_cc: bool
    verification: str
    cli_name: str | None

    @abstractmethod
    async def ensure_cli_installed(self) -> None:
        """
        Check whether the provider's CLI tool is installed and install it if not.

        Should be a no-op when the CLI is already present.  Should raise
        ``RuntimeError`` if installation fails and cannot continue.
        """
        ...

    @abstractmethod
    async def login(self) -> None:
        """
        Authenticate the user with the provider.

        Should raise ``RuntimeError`` on authentication failure.
        """
        ...

    @abstractmethod
    async def provision(
        self,
        db: DatabaseCredentials,
        project_name: str,
    ) -> ComputeCredentials:
        """
        Deploy the Metaflow metadata service and return its public URL.

        Parameters
        ----------
        db:
            Database credentials to pass as environment variables to the
            deployed service.
        project_name:
            A short identifier used to name the service on the provider.

        Returns
        -------
        ComputeCredentials
            Contains the public service URL that the Metaflow client will call.
        """
        ...
