"""
Supabase provider implementations for database, storage, and compute.

Supabase offers a unified platform that can serve all three roles at once,
making it the simplest "all-in-one" stack option.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from rich.console import Console

from ..installer import ensure_cli
from .base import (
    ComputeCredentials,
    ComputeProvider,
    DatabaseCredentials,
    DatabaseProvider,
    StorageCredentials,
    StorageProvider,
)

console = Console()


async def _run_async(cmd: list[str], **kwargs: Any) -> tuple[int, str, str]:
    """Run a command asynchronously and return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    return proc.returncode, stdout_bytes.decode(), stderr_bytes.decode()


def _run_sync(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    """Run a command synchronously, capturing output."""
    return subprocess.run(cmd, text=True, capture_output=True, **kwargs)


async def _require_supabase_login() -> None:
    """Require an existing authenticated Supabase CLI session."""
    rc, _, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
    if rc == 0:
        console.print("[green]Supabase CLI already authenticated; continuing.[/green]")
        return
    details = stderr.strip() or "Supabase CLI is not authenticated."
    raise RuntimeError(
        "Supabase authentication is required before setup.\n"
        "Run `supabase login` in your terminal, complete browser auth, and "
        "re-run `mf-setup`.\n"
        f"Details: {details}"
    )


def _get_org_id() -> str:
    """
    Retrieve the user's Supabase organisation ID.

    Runs ``supabase orgs list --output json`` and returns the first org's ID.
    Raises ``RuntimeError`` if no organisation is found.
    """
    result = _run_sync(["supabase", "orgs", "list", "--output", "json"])
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list Supabase organisations:\n{result.stderr.strip()}")
    try:
        orgs = json.loads(result.stdout)
        if not orgs:
            raise RuntimeError("No Supabase organisations found for the authenticated user.")
        return orgs[0]["id"]
    except (json.JSONDecodeError, KeyError) as exc:
        raise RuntimeError(
            f"Unexpected output from 'supabase orgs list':\n{result.stdout}"
        ) from exc


async def _get_project_host(project_ref: str) -> str:
    """Return the database host for *project_ref* from `supabase projects list`."""
    rc, stdout, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
    if rc != 0:
        raise RuntimeError(f"Could not list Supabase projects (exit {rc}):\n{stderr.strip()}")
    projects = json.loads(stdout) if stdout.strip() else []
    project = next((p for p in projects if p.get("id") == project_ref), None)
    if not project:
        raise RuntimeError(f"Supabase project {project_ref!r} was not found.")

    db_info = project.get("database", {})
    host = ""
    if isinstance(db_info, dict):
        host = str(db_info.get("host", "")).strip()
    if not host:
        host = f"db.{project_ref}.supabase.co"
    return host


def _generate_password(length: int = 32) -> str:
    """Generate a cryptographically random alphanumeric password."""
    return secrets.token_urlsafe(length)


async def _wait_for_project(
    project_ref: str,
    timeout_seconds: int = 120,
    poll_interval: float = 5.0,
) -> None:
    """
    Poll ``supabase projects list`` until the project status is ACTIVE_HEALTHY.

    Raises ``RuntimeError`` if the project is not healthy within
    *timeout_seconds* seconds.
    """
    elapsed = 0.0
    while elapsed < timeout_seconds:
        rc, stdout, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
        if rc == 0 and stdout.strip():
            try:
                projects = json.loads(stdout)
                project = next((p for p in projects if p.get("id") == project_ref), None)
                if project:
                    status: str = project.get("status", "")
                    if status == "ACTIVE_HEALTHY":
                        return
                    if status in {"INACTIVE", "REMOVED"}:
                        raise RuntimeError(
                            f"Supabase project {project_ref!r} entered unexpected "
                            f"status: {status!r}"
                        )
            except json.JSONDecodeError:
                pass
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    raise RuntimeError(
        f"Timed out waiting for Supabase project {project_ref!r} to become "
        f"ACTIVE_HEALTHY (waited {timeout_seconds}s)."
    )


async def _create_storage_bucket(
    project_ref: str,
    bucket_name: str,
    service_key: str,
) -> None:
    """Create a storage bucket via the Supabase Storage API."""
    url = f"https://{project_ref}.supabase.co/storage/v1/bucket"
    headers = {
        "Authorization": f"Bearer {service_key}",
        "apikey": service_key,
        "Content-Type": "application/json",
    }
    payload = {"id": bucket_name, "name": bucket_name, "public": False}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, headers=headers, json=payload)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to create Supabase storage bucket {bucket_name!r}: {exc}"
        ) from exc

    if response.status_code in {200, 201}:
        return

    body = response.text
    if response.status_code in {400, 409} and "already" in body.lower():
        return

    raise RuntimeError(
        f"Failed to create Supabase storage bucket {bucket_name!r} "
        f"(HTTP {response.status_code}):\n{body}"
    )


class SupabaseDatabaseProvider(DatabaseProvider):
    """
    Provisions a Postgres database on Supabase.

    Uses the Supabase CLI to create a project and extract the database
    connection string from the project settings.
    """

    name = "supabase"
    display_name = "Supabase (serverless Postgres, free tier)"
    requires_cc = False
    verification = "email"
    cli_name = "supabase"

    async def ensure_cli_installed(self) -> None:
        """Install the Supabase CLI if not already on the PATH."""
        await ensure_cli("supabase")

    async def login(self) -> None:
        """
        Log in to Supabase via the CLI browser-based OAuth flow.

        Opens a browser window for OAuth and waits for the CLI to confirm
        authentication.
        """
        await self.ensure_cli_installed()
        await _require_supabase_login()

    async def provision(self, project_name: str) -> DatabaseCredentials:
        """
        Create a new Supabase project and return Postgres credentials.

        If a project with the same name already exists, its credentials are
        returned without creating a new one.
        """
        await self.ensure_cli_installed()

        # Check for an existing project with this name.
        rc, stdout, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
        projects: list[dict[str, Any]] = []
        if rc == 0 and stdout.strip():
            try:
                projects = json.loads(stdout)
            except json.JSONDecodeError:
                projects = []

        existing = next((p for p in projects if p.get("name") == project_name), None)

        db_password = ""
        if existing:
            project_ref: str = existing["id"]
            console.print(f"[yellow]Reusing existing Supabase project:[/yellow] {project_ref}")
            db_password = os.environ.get("SUPABASE_DB_PASSWORD", "").strip()
            if not db_password:
                raise RuntimeError(
                    "Existing Supabase project detected, but DB password is unknown.\n"
                    "Set SUPABASE_DB_PASSWORD to that project's Postgres password "
                    "and re-run setup, or choose a new project name."
                )
        else:
            org_id = _get_org_id()
            db_password = _generate_password()

            console.print(f"[bold]Creating Supabase project:[/bold] {project_name!r}")
            rc, stdout, stderr = await _run_async(
                [
                    "supabase",
                    "projects",
                    "create",
                    project_name,
                    "--org-id",
                    org_id,
                    "--db-password",
                    db_password,
                    "--region",
                    "us-east-1",
                    "--output",
                    "json",
                ]
            )
            if rc != 0:
                raise RuntimeError(
                    f"Failed to create Supabase project {project_name!r} "
                    f"(exit {rc}):\n{stderr.strip()}"
                )
            try:
                project_data = json.loads(stdout)
                project_ref = project_data["id"]
            except (json.JSONDecodeError, KeyError) as exc:
                raise RuntimeError(
                    f"Unexpected output from 'supabase projects create':\n{stdout}"
                ) from exc

            # Wait for the project to be fully ready.
            console.print(
                "[bold]Waiting for Supabase project to become ready "
                "(this may take up to 2 minutes)...[/bold]"
            )
            await _wait_for_project(project_ref, timeout_seconds=120)

        host = await _get_project_host(project_ref)
        username = "postgres"
        database = "postgres"
        dsn = (
            f"postgresql://{username}:{quote(db_password, safe='')}@{host}:5432/"
            f"{database}?sslmode=require"
        )

        parsed = urlparse(dsn)
        return DatabaseCredentials(
            dsn=dsn,
            host=parsed.hostname or "",
            port=parsed.port or 5432,
            database=(parsed.path or "/postgres").lstrip("/"),
            username=parsed.username or "postgres",
            password=parsed.password or "",
        )


async def _register_s3_credentials_in_db(
    dsn: str,
    access_key_id: str,
    secret_access_key: str,
    description: str = "metaflow",
) -> bool:
    """
    Insert an S3 key pair into Supabase Storage's ``storage.s3_credentials`` table.

    Returns ``True`` if the row was inserted successfully, ``False`` if the table
    does not exist (e.g. older Supabase version without S3 support).
    Raises ``RuntimeError`` for other database errors.
    """
    try:
        import asyncpg  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "asyncpg is required for registering S3 credentials. "
            "Install it with: pip install asyncpg"
        ) from exc

    try:
        conn = await asyncpg.connect(dsn)
    except Exception as exc:
        raise RuntimeError(f"Could not connect to database: {exc}") from exc

    try:
        # Check the table exists before inserting.
        exists = await conn.fetchval(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'storage' AND table_name = 's3_credentials'
            """
        )
        if not exists:
            return False

        # Remove any previous metaflow-managed credentials and insert fresh ones.
        await conn.execute(
            "DELETE FROM storage.s3_credentials WHERE description = $1",
            description,
        )
        await conn.execute(
            """
            INSERT INTO storage.s3_credentials
                (access_key_id, access_key_secret, description)
            VALUES ($1, $2, $3)
            """,
            access_key_id,
            secret_access_key,
            description,
        )
        return True
    finally:
        await conn.close()


class SupabaseStorageProvider(StorageProvider):
    """
    Provisions an S3-compatible storage bucket on Supabase Storage.

    Supabase Storage exposes an S3-compatible API backed by its own Postgres
    table (``storage.s3_credentials``).  Proper S3 access requires dedicated
    key pairs registered in that table — JWT API tokens do NOT work as HMAC
    credentials.

    Call ``set_db_dsn(dsn)`` before ``provision()`` so that the provider can
    INSERT the generated credentials into the database.
    """

    name = "supabase"
    display_name = "Supabase Storage (S3-compatible, free tier)"
    requires_cc = False
    verification = "email"
    cli_name = "supabase"

    def __init__(self, project_ref: str | None = None) -> None:
        """
        Parameters
        ----------
        project_ref:
            Supabase project reference ID.  If not provided it will be
            determined from the list of projects at provision time.
        """
        self._project_ref = project_ref
        self._db_dsn: str | None = None

    def set_db_dsn(self, dsn: str) -> None:
        """Provide the Postgres DSN so proper S3 credentials can be registered."""
        self._db_dsn = dsn

    async def ensure_cli_installed(self) -> None:
        """Install the Supabase CLI if not already on the PATH."""
        await ensure_cli("supabase")

    async def login(self) -> None:
        """Require prior Supabase CLI authentication."""
        await self.ensure_cli_installed()
        await _require_supabase_login()

    async def _resolve_project_ref(self) -> str:
        """
        Return the stored project ref or discover it from the projects list.

        Raises ``RuntimeError`` if no projects are found and no ref was
        provided at construction time.
        """
        if self._project_ref:
            return self._project_ref

        rc, stdout, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
        if rc != 0:
            raise RuntimeError(f"Could not list Supabase projects (exit {rc}):\n{stderr.strip()}")
        projects = json.loads(stdout) if stdout.strip() else []
        if not projects:
            raise RuntimeError(
                "No Supabase projects found. Run the database provider first "
                "or pass project_ref to SupabaseStorageProvider()."
            )
        # Use the most recently created project (last in the list).
        self._project_ref = projects[-1]["id"]
        return self._project_ref

    async def provision(self, bucket_name: str) -> StorageCredentials:
        """
        Create a Supabase Storage bucket and return proper S3 credentials.

        Supabase Storage validates S3 requests via HMAC against key pairs stored
        in ``storage.s3_credentials``.  This method generates a dedicated key
        pair and inserts it into that table using the DB DSN supplied via
        ``set_db_dsn()``.  JWT API tokens are used only for the Storage REST API
        calls (bucket creation), not as S3 credentials.
        """
        await self.ensure_cli_installed()

        project_ref = await self._resolve_project_ref()
        console.print(
            f"[bold]Provisioning Supabase Storage bucket[/bold] "
            f"{bucket_name!r} on project {project_ref}"
        )

        endpoint_url = f"https://{project_ref}.supabase.co/storage/v1/s3"

        # Retrieve the service_role key for Storage REST API calls (bucket creation).
        rc, stdout, stderr = await _run_async(
            [
                "supabase",
                "projects",
                "api-keys",
                "--project-ref",
                project_ref,
                "--output",
                "json",
            ]
        )
        if rc != 0:
            raise RuntimeError(
                f"Failed to retrieve Supabase project API keys (exit {rc}):\n{stderr.strip()}"
            )

        try:
            keys_data = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Unexpected output from 'supabase projects api-keys':\n{stdout}"
            ) from exc

        keys_by_name: dict[str, str] = {}
        if isinstance(keys_data, list):
            for entry in keys_data:
                keys_by_name[entry.get("name", "")] = entry.get("api_key", "")
        elif isinstance(keys_data, dict):
            keys_by_name = {k: v for k, v in keys_data.items() if isinstance(v, str)}

        service_role_key = keys_by_name.get(
            "service_role", keys_by_name.get("service_role key", "")
        )
        if not service_role_key:
            raise RuntimeError(
                f"Could not find service_role key in Supabase API keys: {list(keys_by_name.keys())}"
            )

        # Create the bucket (idempotent).
        await _create_storage_bucket(project_ref, bucket_name, service_role_key)

        # Generate a proper S3 key pair for HMAC authentication.
        # These are registered in storage.s3_credentials so Supabase's S3 API
        # can validate signed requests.
        access_key_id = secrets.token_urlsafe(20)
        secret_access_key = secrets.token_urlsafe(30)

        if self._db_dsn:
            registered = await _register_s3_credentials_in_db(
                self._db_dsn, access_key_id, secret_access_key, bucket_name
            )
            if registered:
                console.print("[green]S3 credentials registered in storage.s3_credentials.[/green]")
            else:
                console.print(
                    "[yellow]Warning: could not register S3 credentials automatically "
                    "(storage.s3_credentials table not found).[/yellow]\n"
                    "Create S3 credentials manually: Supabase Dashboard → "
                    "Project Settings → Storage → S3 Access Keys, then update "
                    "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in ~/.metaflowconfig."
                )
        else:
            console.print(
                "[yellow]Warning: DB DSN not available; S3 credentials not registered "
                "in database. Storage will not work until credentials are registered.[/yellow]"
            )

        return StorageCredentials(
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket=bucket_name,
            region="us-east-1",
        )


class SupabaseComputeProvider(ComputeProvider):
    """
    Deploys the Metaflow metadata service as a Supabase Edge Function.

    The edge function source is located in the ``edge_function/`` directory
    at the repository root.  It is deployed using the Supabase CLI's
    ``functions deploy`` command.
    """

    name = "supabase"
    display_name = "Supabase (Edge Functions)"
    requires_cc = False
    verification = "email"
    cli_name = "supabase"

    # Name of the edge function as deployed on Supabase.
    _FUNCTION_NAME = "metadata-router"
    _SERVICE_AUTH_ENV = "METAFLOW_SERVICE_AUTH_KEY"

    async def ensure_cli_installed(self) -> None:
        """Install the Supabase CLI if not already on the PATH."""
        await ensure_cli("supabase")

    async def login(self) -> None:
        """Require prior Supabase CLI authentication."""
        await self.ensure_cli_installed()
        await _require_supabase_login()

    def _find_edge_function_dir(self) -> Path:
        """
        Locate the ``edge_function/`` directory relative to this package.

        Walks up from this file until it finds a directory that contains
        ``edge_function/``.  Raises ``RuntimeError`` if not found.
        """
        # This file lives at:
        #   <repo_root>/src/metaflow_serverless/providers/supabase.py
        # The edge function lives at:
        #   <repo_root>/edge_function/
        candidate = Path(__file__).resolve()
        for _ in range(6):  # Walk up at most 6 levels.
            candidate = candidate.parent
            edge_fn_dir = candidate / "edge_function"
            if edge_fn_dir.is_dir():
                return edge_fn_dir
        raise RuntimeError(
            "Could not locate the 'edge_function/' directory relative to "
            f"{Path(__file__)}. "
            "Ensure you are running from within the metaflow-ephemeral-service repo."
        )

    async def provision(
        self,
        db: DatabaseCredentials,
        project_name: str,
    ) -> ComputeCredentials:
        """
        Deploy the Metaflow metadata router edge function to Supabase.

        Finds the edge function source in the repository, sets database
        credentials as function secrets, and deploys via the Supabase CLI.

        Returns the public HTTPS URL of the deployed function.
        """
        await self.ensure_cli_installed()

        # Resolve the project ref.
        rc, stdout, stderr = await _run_async(["supabase", "projects", "list", "--output", "json"])
        if rc != 0:
            raise RuntimeError(f"Could not list Supabase projects (exit {rc}):\n{stderr.strip()}")

        projects: list[dict[str, Any]] = json.loads(stdout) if stdout.strip() else []
        project = next(
            (p for p in projects if p.get("name") == project_name),
            projects[-1] if projects else None,
        )
        if not project:
            raise RuntimeError(
                "No Supabase project found for compute deployment. Run the database provider first."
            )
        project_ref: str = project["id"]
        service_auth_key = os.environ.get(self._SERVICE_AUTH_ENV, "").strip()
        if not service_auth_key:
            # Generate a random API key used by Metaflow clients and mf-ui.
            service_auth_key = secrets.token_urlsafe(32)

        # Set DB credentials as edge function secrets.
        secrets_env = "\n".join(
            [
                f"MF_METADATA_DB_DSN={db.dsn}",
                f"MF_METADATA_DB_HOST={db.host}",
                f"MF_METADATA_DB_PORT={db.port}",
                f"MF_METADATA_DB_USER={db.username}",
                f"MF_METADATA_DB_PASS={db.password}",
                f"MF_METADATA_DB_NAME={db.database}",
                f"{self._SERVICE_AUTH_ENV}={service_auth_key}",
            ]
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as tmp_env:
            tmp_env.write(secrets_env)
            env_file = tmp_env.name

        try:
            rc, stdout, stderr = await _run_async(
                [
                    "supabase",
                    "secrets",
                    "set",
                    "--project-ref",
                    project_ref,
                    "--env-file",
                    env_file,
                ]
            )
        finally:
            os.unlink(env_file)

        if rc != 0:
            # Non-fatal: continue even if secrets set fails (may already be set).
            console.print(
                f"[yellow]Warning: could not set Supabase function secrets "
                f"(exit {rc}):[/yellow] {stderr.strip()}"
            )

        # Build a temporary Supabase CLI project layout that matches current
        # expected function path: supabase/functions/<function-name>/index.ts
        edge_fn_dir = self._find_edge_function_dir()
        with tempfile.TemporaryDirectory(prefix="mf-sb-fn-") as staging_dir:
            staging_root = Path(staging_dir)
            function_dir = staging_root / "supabase" / "functions" / self._FUNCTION_NAME
            function_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(edge_fn_dir, function_dir, dirs_exist_ok=True)

            console.print(
                f"[bold]Deploying edge function[/bold] "
                f"{self._FUNCTION_NAME!r} from {function_dir} "
                f"to project {project_ref}"
            )

            rc, stdout, stderr = await _run_async(
                [
                    "supabase",
                    "functions",
                    "deploy",
                    self._FUNCTION_NAME,
                    "--project-ref",
                    project_ref,
                    "--no-verify-jwt",
                ],
                cwd=str(staging_root),
            )
        if rc != 0:
            raise RuntimeError(
                f"Failed to deploy Supabase edge function "
                f"{self._FUNCTION_NAME!r} (exit {rc}):\n{stderr.strip()}"
            )

        service_url = f"https://{project_ref}.supabase.co/functions/v1/{self._FUNCTION_NAME}"
        console.print(f"[green]Edge function deployed:[/green] {service_url}")
        return ComputeCredentials(
            service_url=service_url,
            service_auth_key=service_auth_key,
        )
