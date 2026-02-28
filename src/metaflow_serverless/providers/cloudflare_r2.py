"""
Cloudflare R2 storage provider.

R2 is Cloudflare's S3-compatible object storage with zero egress fees and a
generous free tier (10 GB storage, 1M Class-A ops/month, 10M Class-B ops/month).

Bucket creation is performed via the Wrangler CLI.  S3 API credentials (an
access key ID and secret) are generated via the Cloudflare REST API because
Wrangler does not expose a subcommand for creating R2 API tokens.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
from rich.console import Console

from ..installer import ensure_cli
from .base import StorageCredentials, StorageProvider

console = Console()

_CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4"


async def _run_async(cmd: list[str], **kwargs: Any) -> tuple[int, str, str]:
    """Run a command asynchronously; return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(), err.decode()


class CloudflareR2Provider(StorageProvider):
    """
    Provisions an S3-compatible bucket on Cloudflare R2.

    Uses the ``wrangler`` CLI to authenticate and create buckets, then calls
    the Cloudflare REST API to generate scoped R2 API tokens.

    Free tier limits:
        - 10 GB storage
        - 1 million Class-A operations/month (writes)
        - 10 million Class-B operations/month (reads)
        - Zero egress fees
    """

    name = "r2"
    display_name = "Cloudflare R2 (S3-compatible, free tier, zero egress)"
    requires_cc = True   # CC required for R2 even on free tier.
    verification = "email"
    cli_name = "wrangler"

    def __init__(self) -> None:
        self._account_id: str | None = None
        # The wrangler OAuth token is stored in wrangler's own config dir;
        # we read it via `wrangler whoami` or the CLOUDFLARE_API_TOKEN env var.
        self._api_token: str | None = None

    async def ensure_cli_installed(self) -> None:
        """Install wrangler (Cloudflare's CLI) via npm if not present."""
        await ensure_cli("wrangler")

    async def login(self) -> None:
        """
        Authenticate with Cloudflare via the wrangler browser-based OAuth flow.

        Opens the user's browser and writes an OAuth token to the wrangler
        config directory.  After login, retrieves and caches the Cloudflare
        account ID and the API token for subsequent REST API calls.
        """
        await self.ensure_cli_installed()
        console.print(
            "[bold]Opening browser for Cloudflare login...[/bold]"
        )
        proc = await asyncio.create_subprocess_exec(
            "wrangler", "login",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"Cloudflare login failed (exit {proc.returncode}):\n"
                f"{stderr.decode().strip()}"
            )

        # Retrieve the account ID and API token.
        await self._fetch_account_id_and_token()

    async def _fetch_account_id_and_token(self) -> None:
        """
        Run ``wrangler whoami`` to obtain the account ID and OAuth token.

        Wrangler stores its OAuth token under ~/.config/wrangler (or the
        platform-specific equivalent).  ``wrangler whoami`` outputs the
        account information and the token can be retrieved from wrangler's
        config file.
        """
        import os

        # Check if the user supplied an API token in the environment.
        env_token = os.environ.get("CLOUDFLARE_API_TOKEN", "").strip()
        env_account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()

        rc, stdout, stderr = await _run_async(["wrangler", "whoami", "--json"])
        if rc != 0:
            # Fall back to env vars.
            if env_account_id and env_token:
                self._account_id = env_account_id
                self._api_token = env_token
                return
            raise RuntimeError(
                f"Could not determine Cloudflare account information "
                f"(exit {rc}):\n{stderr.strip()}"
            )

        try:
            data = json.loads(stdout)
            accounts: list[dict] = data.get("accounts", [])
            if not accounts:
                raise RuntimeError(
                    "No Cloudflare accounts found for the authenticated user."
                )
            self._account_id = env_account_id or accounts[0]["id"]
        except (json.JSONDecodeError, KeyError) as exc:
            raise RuntimeError(
                f"Unexpected output from 'wrangler whoami':\n{stdout}"
            ) from exc

        # Retrieve the OAuth token from wrangler's config.
        if env_token:
            self._api_token = env_token
        else:
            self._api_token = self._read_wrangler_token()

        if not self._api_token:
            raise RuntimeError(
                "Could not locate the Cloudflare API token after wrangler login.\n"
                "Set CLOUDFLARE_API_TOKEN in your environment as a fallback."
            )

    @staticmethod
    def _read_wrangler_token() -> str | None:
        """
        Read the wrangler OAuth access token from its config file.

        Wrangler stores credentials in TOML or JSON format under:
          - macOS/Linux: ~/.config/.wrangler/config/default.toml
          - XDG_CONFIG_HOME/.wrangler/config/default.toml
        """
        import os
        from pathlib import Path

        config_dirs = []
        xdg = os.environ.get("XDG_CONFIG_HOME", "")
        if xdg:
            config_dirs.append(Path(xdg) / ".wrangler" / "config")
        config_dirs.append(Path.home() / ".config" / ".wrangler" / "config")
        config_dirs.append(Path.home() / ".wrangler" / "config")

        for config_dir in config_dirs:
            toml_path = config_dir / "default.toml"
            if toml_path.exists():
                try:
                    content = toml_path.read_text()
                    # Simple line-by-line parse for oauth_token.
                    for line in content.splitlines():
                        line = line.strip()
                        if line.startswith("oauth_token"):
                            _, _, value = line.partition("=")
                            return value.strip().strip('"').strip("'")
                except Exception:
                    continue
        return None

    def _auth_headers(self) -> dict[str, str]:
        """Return HTTP headers for the Cloudflare REST API."""
        if not self._api_token:
            raise RuntimeError(
                "Not authenticated. Call login() before provision()."
            )
        return {
            "Authorization": f"Bearer {self._api_token}",
            "Content-Type": "application/json",
        }

    async def provision(self, bucket_name: str) -> StorageCredentials:
        """
        Create an R2 bucket and generate scoped S3-compatible API credentials.

        The bucket is created via the wrangler CLI.  An R2 API token (access
        key + secret) is generated via the Cloudflare REST API, scoped to the
        specific bucket.

        Parameters
        ----------
        bucket_name:
            Name of the R2 bucket to create.

        Returns
        -------
        StorageCredentials
            S3-compatible credentials pointing at the R2 bucket.
        """
        await self.ensure_cli_installed()

        if not self._account_id:
            await self._fetch_account_id_and_token()

        account_id = self._account_id
        assert account_id  # guarded above

        # Create the R2 bucket via wrangler CLI.
        console.print(
            f"[bold]Creating Cloudflare R2 bucket:[/bold] {bucket_name!r}"
        )
        rc, stdout, stderr = await _run_async(
            ["wrangler", "r2", "bucket", "create", bucket_name]
        )
        if rc != 0 and "already exists" not in (stdout + stderr).lower():
            raise RuntimeError(
                f"Failed to create R2 bucket {bucket_name!r} (exit {rc}):\n"
                f"{stderr.strip()}"
            )

        # Generate an R2 API token via the Cloudflare REST API.
        console.print(
            "[bold]Generating R2 API credentials via Cloudflare API...[/bold]"
        )
        access_key_id, secret_access_key = await self._create_r2_api_token(
            account_id, bucket_name
        )

        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        console.print(
            f"[green]R2 bucket provisioned:[/green] {endpoint_url}/{bucket_name}"
        )
        return StorageCredentials(
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket=bucket_name,
            region="auto",  # R2 uses "auto" as the region.
        )

    async def _create_r2_api_token(
        self, account_id: str, bucket_name: str
    ) -> tuple[str, str]:
        """
        Create an R2 API token scoped to *bucket_name* via the Cloudflare API.

        Returns ``(access_key_id, secret_access_key)``.

        The Cloudflare API endpoint for creating R2 tokens is:
          POST /accounts/{account_id}/r2/tokens
        """
        payload = {
            "name": f"metaflow-{bucket_name[:40]}",
            "policies": [
                {
                    "effect": "allow",
                    "resources": {
                        f"com.cloudflare.edge.r2.bucket.{account_id}_default_{bucket_name}": "*"
                    },
                    "permission_groups": [
                        {"id": "2efd5506f9c8494dacb1fa10a3e7d5b6"},  # Workers R2 Storage Read
                        {"id": "6a018a9f431d4ec6a1f81fda7b609b9c"},  # Workers R2 Storage Write
                    ],
                }
            ],
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{_CLOUDFLARE_API_BASE}/accounts/{account_id}/r2/tokens",
                json=payload,
                headers=self._auth_headers(),
            )

        if response.status_code == 403:
            raise RuntimeError(
                "Cloudflare API returned 403 Forbidden when creating R2 token.\n"
                "Ensure your API token has the 'R2 Token' permission."
            )
        response.raise_for_status()

        data = response.json()
        if not data.get("success"):
            errors = data.get("errors", [])
            raise RuntimeError(
                f"Cloudflare API error when creating R2 token: {errors}"
            )

        result = data.get("result", {})
        access_key_id: str = result.get("accessKeyId", "")
        secret_access_key: str = result.get("secretAccessKey", "")

        if not access_key_id or not secret_access_key:
            raise RuntimeError(
                f"Cloudflare API returned incomplete R2 token data: {result!r}"
            )

        return access_key_id, secret_access_key
