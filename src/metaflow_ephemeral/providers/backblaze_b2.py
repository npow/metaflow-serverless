"""
Backblaze B2 storage provider.

Backblaze B2 offers S3-compatible object storage with a free tier of 10 GB
and 1 GB/day of free egress.  This provider uses the Backblaze B2 REST API
directly (no CLI required) via application keys.
"""

from __future__ import annotations

import asyncio
import base64
import os

import httpx
from rich.console import Console

from .base import StorageCredentials, StorageProvider

console = Console()

_KEY_ID_ENV = "B2_APPLICATION_KEY_ID"
_KEY_ENV = "B2_APPLICATION_KEY"
_AUTH_URL = "https://api.backblazeb2.com/b2api/v3/b2_authorize_account"


class BackblazeB2Provider(StorageProvider):
    """
    Provisions S3-compatible storage on Backblaze B2.

    Authentication uses Backblaze application key ID and application key,
    provided either via environment variables (``B2_APPLICATION_KEY_ID``
    and ``B2_APPLICATION_KEY``) or via interactive prompt.

    The S3-compatible endpoint for B2 is:
        https://s3.<region>.backblazeb2.com

    Free tier:
        - 10 GB storage
        - 1 GB/day of free download
        - Class-A transactions free up to 2,500/day
        - No credit card required; phone verification needed
    """

    name = "b2"
    display_name = "Backblaze B2 (S3-compatible, 10 GB free)"
    requires_cc = False
    verification = "phone"  # Backblaze requires phone verification.
    cli_name = None  # Uses REST API directly.

    def __init__(self) -> None:
        self._key_id: str | None = None
        self._key: str | None = None
        # Populated after b2_authorize_account succeeds.
        self._api_url: str | None = None
        self._account_id: str | None = None
        self._s3_api_url: str | None = None
        self._auth_token: str | None = None

    async def ensure_cli_installed(self) -> None:
        """No CLI is required for Backblaze B2; this is a no-op."""
        return

    async def login(self) -> None:
        """
        Authenticate with Backblaze using an application key.

        Reads credentials from environment variables first; falls back to
        interactive prompts.  After successful authentication the Backblaze
        API URL and account ID are stored for subsequent calls.

        Users can create application keys at:
          https://secure.backblaze.com/app_keys.htm
        """
        key_id = os.environ.get(_KEY_ID_ENV, "").strip()
        key = os.environ.get(_KEY_ENV, "").strip()

        if not key_id or not key:
            console.print(
                "\n[bold]Backblaze B2 requires an application key for provisioning.[/bold]\n"
                "Create one at: [link]https://secure.backblaze.com/app_keys.htm[/link]\n"
                "You need both the [italic]keyID[/italic] and the "
                "[italic]applicationKey[/italic].\n"
            )
            try:
                key_id = input("Paste your B2 Application Key ID: ").strip()
                key = input("Paste your B2 Application Key: ").strip()
            except (EOFError, KeyboardInterrupt):
                raise RuntimeError("Backblaze B2 login cancelled by user.")

        if not key_id or not key:
            raise RuntimeError(
                "No Backblaze credentials provided; cannot authenticate."
            )

        self._key_id = key_id
        self._key = key

        await self._authorize()
        console.print("[green]Backblaze B2 authentication successful.[/green]")

    async def _authorize(self) -> None:
        """
        Call b2_authorize_account to obtain an auth token and API URLs.

        Stores the token, API URL, account ID, and S3-compatible API URL for
        later use.
        """
        credentials = base64.b64encode(
            f"{self._key_id}:{self._key}".encode()
        ).decode()

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                _AUTH_URL,
                headers={"Authorization": f"Basic {credentials}"},
            )

        if response.status_code == 401:
            raise RuntimeError(
                "Backblaze authentication failed: invalid application key."
            )
        response.raise_for_status()

        data = response.json()
        self._auth_token = data["authorizationToken"]
        self._api_url = data["apiInfo"]["storageApi"]["apiUrl"]
        self._account_id = data["accountId"]
        # B2 includes an S3-compatible endpoint in the auth response.
        self._s3_api_url = data["apiInfo"]["storageApi"].get(
            "s3ApiUrl",
            "https://s3.us-east-005.backblazeb2.com",
        )

    async def provision(self, bucket_name: str) -> StorageCredentials:
        """
        Create a Backblaze B2 bucket and return S3-compatible credentials.

        If a bucket with the given name already exists (for this account), it
        is reused.  A scoped application key is created for the bucket.

        Parameters
        ----------
        bucket_name:
            Name of the B2 bucket to create.  Must be globally unique across
            all Backblaze accounts (not just yours).

        Returns
        -------
        StorageCredentials
            S3-compatible credentials for the bucket.
        """
        if not self._auth_token:
            raise RuntimeError("Call login() before provision().")

        console.print(
            f"[bold]Provisioning Backblaze B2 bucket:[/bold] {bucket_name!r}"
        )

        async with httpx.AsyncClient(timeout=60.0) as client:
            headers = {
                "Authorization": self._auth_token,
                "Content-Type": "application/json",
            }

            # List existing buckets to check for one with this name.
            list_response = await client.post(
                f"{self._api_url}/b2api/v3/b2_list_buckets",
                json={"accountId": self._account_id},
                headers=headers,
            )
            list_response.raise_for_status()
            buckets: list[dict] = list_response.json().get("buckets", [])
            existing = next(
                (b for b in buckets if b.get("bucketName") == bucket_name),
                None,
            )

            if existing:
                bucket_id: str = existing["bucketId"]
                region = self._region_from_endpoint(self._s3_api_url or "")
                console.print(
                    f"[yellow]Reusing existing B2 bucket:[/yellow] {bucket_name!r}"
                )
            else:
                # Create the bucket with private access.
                create_response = await client.post(
                    f"{self._api_url}/b2api/v3/b2_create_bucket",
                    json={
                        "accountId": self._account_id,
                        "bucketName": bucket_name,
                        "bucketType": "allPrivate",
                    },
                    headers=headers,
                )
                create_response.raise_for_status()
                create_data = create_response.json()
                bucket_id = create_data["bucketId"]
                region = self._region_from_endpoint(self._s3_api_url or "")
                console.print(
                    f"[green]B2 bucket created:[/green] {bucket_name!r}"
                )

            # Create a scoped application key for this bucket.
            console.print(
                "[bold]Creating scoped application key for bucket...[/bold]"
            )
            key_response = await client.post(
                f"{self._api_url}/b2api/v3/b2_create_key",
                json={
                    "accountId": self._account_id,
                    "capabilities": [
                        "listBuckets",
                        "listFiles",
                        "readFiles",
                        "writeFiles",
                        "deleteFiles",
                    ],
                    "keyName": f"metaflow-{bucket_name[:40]}",
                    "bucketId": bucket_id,
                },
                headers=headers,
            )
            key_response.raise_for_status()
            key_data = key_response.json()

        access_key_id: str = key_data["applicationKeyId"]
        secret_access_key: str = key_data["applicationKey"]
        endpoint_url = self._s3_api_url or "https://s3.us-east-005.backblazeb2.com"

        console.print(
            f"[green]Backblaze B2 provisioning complete.[/green] "
            f"Endpoint: {endpoint_url}"
        )
        return StorageCredentials(
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket=bucket_name,
            region=region,
        )

    @staticmethod
    def _region_from_endpoint(endpoint: str) -> str:
        """
        Extract the region string from a B2 S3-compatible endpoint URL.

        Example: "https://s3.us-east-005.backblazeb2.com" -> "us-east-005"
        Falls back to "us-east-005" if parsing fails.
        """
        try:
            host = endpoint.replace("https://", "").replace("http://", "")
            parts = host.split(".")
            # Expected format: s3.<region>.backblazeb2.com
            if len(parts) >= 4 and parts[0] == "s3":
                return parts[1]
        except Exception:
            pass
        return "us-east-005"
