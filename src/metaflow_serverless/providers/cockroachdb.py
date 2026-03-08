"""
CockroachDB Cloud provider implementation for database.

CockroachDB Serverless provides a free-tier distributed Postgres-compatible
database with automatic scaling.  This provider uses the CockroachDB Cloud
CLI (ccloud) for authentication and provisioning.
"""

from __future__ import annotations

import asyncio
import json
import platform
import shutil
import stat
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx
from rich.console import Console

from .base import DatabaseCredentials, DatabaseProvider

console = Console()

# Download URL template for the ccloud binary.
_CCLOUD_DOWNLOAD_BASE = "https://binaries.cockroachdb.com/cockroach-cloud"


async def _run_async(cmd: list[str], **kwargs) -> tuple[int, str, str]:
    """Run a command asynchronously; return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    out, err = await proc.communicate()
    return proc.returncode or 0, out.decode(), err.decode()


async def _install_ccloud() -> None:
    """
    Download and install the ``ccloud`` binary from CockroachDB Labs.

    Determines the correct platform binary URL, downloads it to
    ``~/.local/bin/ccloud``, and makes it executable.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        os_token = "darwin"
        arch_token = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    elif system == "linux":
        os_token = "linux"
        arch_token = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    else:
        raise RuntimeError(
            f"Automatic ccloud installation is not supported on {system!r}.\n"
            "Download the CockroachDB Cloud CLI manually from:\n"
            "  https://www.cockroachlabs.com/docs/cockroachcloud/ccloud-get-started"
        )

    # The CockroachDB Labs binary download page follows the pattern:
    #   https://binaries.cockroachdb.com/cockroach-cloud/ccloud_<version>_<os>_<arch>.tar.gz
    # Fetch the latest version from the GitHub releases API.
    github_api = "https://api.github.com/repos/cockroachdb/cockroach-cloud-cli/releases/latest"
    console.print("[bold]Fetching latest ccloud release...[/bold]")
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(
                github_api,
                headers={"Accept": "application/vnd.github+json"},
            )
            if response.status_code == 200:
                release = response.json()
                assets: list[dict] = release.get("assets", [])
                # Find the matching asset.
                asset_url: str | None = None
                for asset in assets:
                    name: str = asset.get("name", "").lower()
                    if (
                        os_token in name
                        and arch_token in name
                        and not any(
                            name.endswith(ext) for ext in (".sha256", ".sig", ".asc", ".md5")
                        )
                    ):
                        asset_url = asset["browser_download_url"]
                        break
            else:
                asset_url = None
        except Exception:
            asset_url = None

    if not asset_url:
        raise RuntimeError(
            "Could not find a ccloud binary for your platform.\n"
            "Please install the CockroachDB Cloud CLI manually:\n"
            "  https://www.cockroachlabs.com/docs/cockroachcloud/ccloud-get-started"
        )

    local_bin = Path.home() / ".local" / "bin"
    local_bin.mkdir(parents=True, exist_ok=True)
    dest = local_bin / "ccloud"

    console.print(f"[bold]Downloading ccloud from[/bold] {asset_url}")
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        dl = await client.get(asset_url)
        dl.raise_for_status()
        data = dl.content

    asset_name = asset_url.split("/")[-1]
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        archive_path = tmp_path / asset_name

        archive_path.write_bytes(data)

        # Extract.
        import tarfile
        import zipfile

        if asset_name.endswith(".tar.gz") or asset_name.endswith(".tgz"):
            with tarfile.open(archive_path, "r:gz") as tf:
                tf.extractall(tmp_path)
        elif asset_name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(tmp_path)
        else:
            # Bare binary.
            import shutil as _shutil

            _shutil.copy2(archive_path, dest)
            dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            console.print(f"[green]ccloud installed to[/green] {dest}")
            return

        # Find ccloud binary in the extracted tree.
        binary_path: Path | None = None
        for candidate in tmp_path.rglob("ccloud"):
            if candidate.is_file():
                binary_path = candidate
                break
        if binary_path is None:
            raise RuntimeError(f"Could not find 'ccloud' binary inside archive {asset_name!r}.")

        import shutil as _shutil

        _shutil.copy2(binary_path, dest)
        dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    console.print(f"[green]ccloud installed to[/green] {dest}")

    # Ensure ~/.local/bin is on PATH for the current process.
    import os

    local_bin_str = str(local_bin)
    current_path = os.environ.get("PATH", "")
    if local_bin_str not in current_path.split(os.pathsep):
        os.environ["PATH"] = local_bin_str + os.pathsep + current_path


class CockroachDBProvider(DatabaseProvider):
    """
    Provisions a CockroachDB Serverless cluster using the ``ccloud`` CLI.

    Authentication is performed via browser-based OAuth through the ccloud CLI.
    The cluster is created on GCP in the us-east1 region on the BASIC (free)
    plan.
    """

    name = "cockroachdb"
    display_name = "CockroachDB Serverless (distributed Postgres, free tier)"
    requires_cc = False
    verification = "email"
    cli_name = "ccloud"

    async def ensure_cli_installed(self) -> None:
        """Install the ccloud CLI if it is not already on the PATH."""
        if shutil.which("ccloud"):
            return
        console.print("[bold]ccloud[/bold] not found on PATH; attempting installation...")
        await _install_ccloud()
        if not shutil.which("ccloud"):
            raise RuntimeError(
                "ccloud installation appeared to succeed but the binary still "
                "cannot be found. Try adding ~/.local/bin to your PATH:\n"
                '  export PATH="$HOME/.local/bin:$PATH"'
            )

    async def login(self) -> None:
        """
        Authenticate with CockroachDB Cloud via the ccloud CLI browser OAuth flow.

        The ccloud CLI provides two login subcommands:
          - ``ccloud auth login --no-redirect``: prints a URL for manual copy-paste.
          - ``ccloud auth login``: opens the browser automatically.

        We attempt the automatic browser flow; users can use the manual URL if
        the browser cannot be opened.
        """
        await self.ensure_cli_installed()

        console.print("[bold]Logging in to CockroachDB Cloud (opening browser)...[/bold]")

        # First, try the interactive browser-based flow.
        rc, stdout, stderr = await _run_async(["ccloud", "auth", "login"])
        if rc == 0:
            console.print("[green]CockroachDB Cloud login successful.[/green]")
            return

        # If the browser flow failed (e.g. headless environment), fall back to
        # the no-redirect (copy-paste URL) flow.
        console.print(
            f"[yellow]Browser login failed (exit {rc}); trying --no-redirect flow...[/yellow]"
        )
        rc2, stdout2, stderr2 = await _run_async(["ccloud", "auth", "login", "--no-redirect"])
        if rc2 != 0:
            raise RuntimeError(f"CockroachDB Cloud login failed (exit {rc2}):\n{stderr2.strip()}")
        console.print("[green]CockroachDB Cloud login successful.[/green]")

    async def provision(self, project_name: str) -> DatabaseCredentials:
        """
        Create or reuse a CockroachDB Serverless cluster named *project_name*.

        Uses the BASIC plan (free tier) on GCP us-east1.  If a cluster with
        the same name already exists, its connection string is returned
        without creating a duplicate.

        Parameters
        ----------
        project_name:
            Used as the cluster name on CockroachDB Cloud.

        Returns
        -------
        DatabaseCredentials
            Postgres-compatible credentials for the provisioned cluster.
        """
        await self.ensure_cli_installed()

        # Check whether a cluster with this name already exists.
        rc, stdout, stderr = await _run_async(["ccloud", "cluster", "list", "--output", "json"])
        existing_id: str | None = None
        if rc == 0 and stdout.strip():
            try:
                clusters = json.loads(stdout)
                # ccloud returns a list or {"clusters": [...]}
                cluster_list = (
                    clusters if isinstance(clusters, list) else clusters.get("clusters", [])
                )
                for cluster in cluster_list:
                    if cluster.get("name") == project_name:
                        existing_id = cluster.get("id") or cluster.get("cluster_id")
                        break
            except (json.JSONDecodeError, KeyError):
                pass

        if existing_id:
            console.print(
                f"[yellow]Reusing existing CockroachDB cluster:[/yellow] "
                f"{project_name!r} ({existing_id})"
            )
            cluster_id = existing_id
        else:
            console.print(f"[bold]Creating CockroachDB cluster:[/bold] {project_name!r}")
            rc, stdout, stderr = await _run_async(
                [
                    "ccloud",
                    "cluster",
                    "create",
                    "--name",
                    project_name,
                    "--cloud",
                    "GCP",
                    "--region",
                    "us-east1",
                    "--plan",
                    "BASIC",
                    "--output",
                    "json",
                ]
            )
            if rc != 0:
                raise RuntimeError(
                    f"Failed to create CockroachDB cluster {project_name!r} "
                    f"(exit {rc}):\n{stderr.strip()}"
                )
            try:
                create_data = json.loads(stdout)
                # ccloud may return the cluster under a "cluster" key or bare.
                cluster_obj = (
                    create_data.get("cluster", create_data)
                    if isinstance(create_data, dict)
                    else create_data
                )
                cluster_id = cluster_obj.get("id") or cluster_obj.get("cluster_id")
                if not cluster_id:
                    raise KeyError("id")
            except (json.JSONDecodeError, KeyError) as exc:
                raise RuntimeError(
                    f"Unexpected output from 'ccloud cluster create':\n{stdout}"
                ) from exc

            console.print("[bold]Waiting for cluster to become ready...[/bold]")
            await self._wait_for_cluster(cluster_id)

        # Retrieve the connection string.
        console.print(f"[bold]Retrieving connection string for cluster[/bold] {cluster_id}")
        rc, stdout, stderr = await _run_async(
            [
                "ccloud",
                "cluster",
                "connection-string",
                cluster_id,
                "--output",
                "json",
            ]
        )
        if rc != 0:
            raise RuntimeError(
                f"Failed to get connection string for cluster {cluster_id!r} "
                f"(exit {rc}):\n{stderr.strip()}"
            )

        try:
            conn_data = json.loads(stdout)
            dsn: str = (
                conn_data
                if isinstance(conn_data, str)
                else conn_data.get(
                    "connection_string",
                    conn_data.get("uri", conn_data.get("url", "")),
                )
            )
        except json.JSONDecodeError:
            dsn = stdout.strip().strip('"')

        if not dsn.startswith("postgresql://") and not dsn.startswith("postgres://"):
            raise RuntimeError(f"ccloud returned an unexpected connection string: {dsn!r}")

        parsed = urlparse(dsn)
        return DatabaseCredentials(
            dsn=dsn,
            host=parsed.hostname or "",
            port=parsed.port or 26257,
            database=(parsed.path or "/defaultdb").lstrip("/"),
            username=parsed.username or "",
            password=parsed.password or "",
        )

    async def _wait_for_cluster(
        self,
        cluster_id: str,
        timeout_seconds: int = 300,
        poll_interval: float = 5.0,
    ) -> None:
        """
        Poll ``ccloud cluster get`` until the cluster state is CREATED.

        Raises ``RuntimeError`` if the cluster does not become ready within
        *timeout_seconds* seconds.
        """
        elapsed = 0.0
        while elapsed < timeout_seconds:
            rc, stdout, stderr = await _run_async(
                ["ccloud", "cluster", "get", cluster_id, "--output", "json"]
            )
            if rc == 0 and stdout.strip():
                try:
                    data = json.loads(stdout)
                    cluster_obj = data.get("cluster", data) if isinstance(data, dict) else data
                    state: str = cluster_obj.get("state", "")
                    if state == "CREATED":
                        return
                    if state in {"DELETED", "FAILED"}:
                        raise RuntimeError(
                            f"CockroachDB cluster {cluster_id!r} entered "
                            f"unexpected state: {state!r}"
                        )
                except json.JSONDecodeError:
                    pass
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise RuntimeError(
            f"Timed out waiting for CockroachDB cluster {cluster_id!r} to "
            f"become ready (waited {timeout_seconds}s)."
        )
