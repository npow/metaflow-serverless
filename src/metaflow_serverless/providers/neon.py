"""
Neon provider implementation for serverless Postgres.

Neon offers a generous free tier with branching, autoscaling, and a dedicated
CLI (neonctl) that makes programmatic provisioning straightforward.
"""

from __future__ import annotations

import asyncio
import json
from urllib.parse import urlparse

from rich.console import Console

from ..installer import ensure_cli
from .base import DatabaseCredentials, DatabaseProvider

console = Console()


async def _run_async(cmd: list[str], **kwargs) -> tuple[int, str, str]:
    """Run a command asynchronously; return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(), err.decode()


class NeonProvider(DatabaseProvider):
    """
    Provisions a serverless Postgres database on Neon.

    Uses the ``neonctl`` CLI to create projects and retrieve connection strings.
    Neon's free tier includes one project with 0.5 GB of storage and automatic
    compute scaling (including scale-to-zero).
    """

    name = "neon"
    display_name = "Neon (serverless Postgres, free tier)"
    requires_cc = False
    verification = "email"
    cli_name = "neonctl"

    async def ensure_cli_installed(self) -> None:
        """Install neonctl if it is not already on the PATH."""
        await ensure_cli("neonctl")

    async def login(self) -> None:
        """
        Authenticate with Neon via the CLI browser-based OAuth flow.

        Opens the user's browser and waits for the CLI to write an API token
        to the local config directory (~/.config/neonctl/).
        """
        await self.ensure_cli_installed()
        console.print("[bold]Opening browser for Neon login...[/bold]")
        rc, stdout, stderr = await _run_async(["neonctl", "auth"])
        if rc != 0:
            raise RuntimeError(f"Neon authentication failed (exit {rc}):\n{stderr.strip()}")
        console.print("[green]Neon authentication successful.[/green]")

    async def provision(self, project_name: str) -> DatabaseCredentials:
        """
        Create a Neon project and return Postgres connection credentials.

        If a project named *project_name* already exists, its primary branch
        connection string is returned without creating a duplicate.

        Parameters
        ----------
        project_name:
            Used as the Neon project name (must be unique within the account).

        Returns
        -------
        DatabaseCredentials
            Full credentials including DSN, host, port, database, username,
            and password.
        """
        await self.ensure_cli_installed()

        # Check for an existing project with this name.
        rc, stdout, stderr = await _run_async(["neonctl", "projects", "list", "--output", "json"])
        existing_id: str | None = None
        if rc == 0 and stdout.strip():
            try:
                projects = json.loads(stdout)
                # neonctl returns {"projects": [...]} or a bare list.
                project_list = (
                    projects if isinstance(projects, list) else projects.get("projects", [])
                )
                for proj in project_list:
                    if proj.get("name") == project_name:
                        existing_id = proj["id"]
                        break
            except (json.JSONDecodeError, KeyError):
                pass

        if existing_id:
            project_id = existing_id
            console.print(
                f"[yellow]Reusing existing Neon project:[/yellow] {project_name!r} ({project_id})"
            )
        else:
            console.print(f"[bold]Creating Neon project:[/bold] {project_name!r}")
            rc, stdout, stderr = await _run_async(
                [
                    "neonctl",
                    "projects",
                    "create",
                    "--name",
                    project_name,
                    "--region-id",
                    "aws-us-east-2",
                    "--output",
                    "json",
                ]
            )
            if rc != 0:
                raise RuntimeError(
                    f"Failed to create Neon project {project_name!r} (exit {rc}):\n{stderr.strip()}"
                )
            try:
                create_data = json.loads(stdout)
                # neonctl returns {"project": {...}, "connection_uris": [...]}
                project_id = create_data["project"]["id"]
            except (json.JSONDecodeError, KeyError) as exc:
                raise RuntimeError(
                    f"Unexpected neonctl output when creating project:\n{stdout}"
                ) from exc

        # Retrieve the primary branch connection string.
        console.print(f"[bold]Retrieving connection string for project[/bold] {project_id}")
        rc, stdout, stderr = await _run_async(
            [
                "neonctl",
                "connection-string",
                "--project-id",
                project_id,
                "--output",
                "json",
            ]
        )
        if rc != 0:
            raise RuntimeError(
                f"Failed to retrieve connection string for project "
                f"{project_id!r} (exit {rc}):\n{stderr.strip()}"
            )

        # neonctl may return a bare string or {"uri": "..."}
        raw = stdout.strip()
        try:
            conn_data = json.loads(raw)
            dsn = conn_data if isinstance(conn_data, str) else conn_data.get("uri", "")
        except json.JSONDecodeError:
            dsn = raw.strip('"')

        if not dsn.startswith("postgresql://") and not dsn.startswith("postgres://"):
            raise RuntimeError(f"neonctl returned an unexpected connection string: {dsn!r}")

        parsed = urlparse(dsn)
        return DatabaseCredentials(
            dsn=dsn,
            host=parsed.hostname or "",
            port=parsed.port or 5432,
            database=(parsed.path or "/neondb").lstrip("/"),
            username=parsed.username or "neondb_owner",
            password=parsed.password or "",
        )
