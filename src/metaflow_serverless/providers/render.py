"""
Render compute provider.

Deploys the Metaflow metadata service as a Render Web Service (Docker-based).
Render's free tier includes one web service that spins down after inactivity
but has no monthly cost.

Provisioning is done via the Render REST API using an API key.
"""

from __future__ import annotations

import asyncio
import os

import httpx
from rich.console import Console

from .base import ComputeCredentials, ComputeProvider, DatabaseCredentials

console = Console()

_API_BASE = "https://api.render.com/v1"
_API_KEY_ENV = "RENDER_API_KEY"

# Public Docker image for the Metaflow metadata service.
_METADATA_IMAGE = "netflixoss/metaflow-metadata-service:latest"


class RenderProvider(ComputeProvider):
    """
    Deploys the Metaflow metadata service as a Render web service.

    Authentication uses a Render API key provided via the ``RENDER_API_KEY``
    environment variable or via interactive prompt.  API keys can be generated
    at https://dashboard.render.com/u/settings → API Keys.

    Free tier:
        - One web service (sleeps after 15 minutes of inactivity)
        - 750 hours/month of active compute
        - 100 GB/month outbound bandwidth

    Note: Render requires a credit card to deploy services (even on the free
    tier) and account verification via email.
    """

    name = "render"
    display_name = "Render (managed containers, free tier)"
    requires_cc = True  # Render requires CC even for free tier deployments.
    verification = "email+cc"
    cli_name = None  # Uses REST API directly.

    def __init__(self) -> None:
        self._api_key: str | None = None
        self._owner_id: str | None = None  # Render team/user ID.

    async def ensure_cli_installed(self) -> None:
        """No CLI required for Render; this is a no-op."""
        return

    async def login(self) -> None:
        """
        Obtain a Render API key.

        Reads from the ``RENDER_API_KEY`` environment variable first; falls
        back to an interactive prompt.  API keys can be generated at:
        https://dashboard.render.com/u/settings#apikeys
        """
        api_key = os.environ.get(_API_KEY_ENV, "").strip()

        if not api_key:
            console.print(
                "\n[bold]Render requires an API key for provisioning.[/bold]\n"
                "Generate one at: [link]https://dashboard.render.com/u/settings#apikeys[/link]\n"
            )
            try:
                api_key = input("Paste your Render API key: ").strip()
            except (EOFError, KeyboardInterrupt) as exc:
                raise RuntimeError("Render login cancelled by user.") from exc

        if not api_key:
            raise RuntimeError("No Render API key provided; cannot authenticate.")

        self._api_key = api_key
        os.environ[_API_KEY_ENV] = api_key

        # Validate the key and retrieve the owner ID.
        await self._fetch_owner_id()
        console.print("[green]Render API key validated.[/green]")

    def _headers(self) -> dict[str, str]:
        """Return HTTP headers for the Render REST API."""
        if not self._api_key:
            raise RuntimeError("Not authenticated. Call login() before provision().")
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _fetch_owner_id(self) -> None:
        """Retrieve and cache the authenticated user/team owner ID."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{_API_BASE}/owners",
                headers=self._headers(),
            )
        if response.status_code == 401:
            raise RuntimeError("Render API key is invalid or expired.")
        response.raise_for_status()
        owners = response.json()
        if not owners:
            raise RuntimeError("No owners found for the Render API key.")
        # Use the first owner (user or team).
        self._owner_id = owners[0]["owner"]["id"]

    async def _find_existing_service(
        self,
        client: httpx.AsyncClient,
        service_name: str,
    ) -> str | None:
        """Return the service ID if a service named *service_name* already exists."""
        response = await client.get(
            f"{_API_BASE}/services",
            params={"name": service_name, "limit": 20},
            headers=self._headers(),
        )
        response.raise_for_status()
        services = response.json()
        for item in services:
            svc = item.get("service", {})
            if svc.get("name") == service_name:
                return svc["id"]
        return None

    async def provision(
        self,
        db: DatabaseCredentials,
        project_name: str,
    ) -> ComputeCredentials:
        """
        Create or update a Render web service running the Metaflow metadata container.

        If a service with the derived name already exists, its environment
        variables are updated and a redeploy is triggered.  Otherwise a new
        web service is created with a Docker image.

        Parameters
        ----------
        db:
            Database credentials passed as environment variables to the service.
        project_name:
            Used to name the Render service.

        Returns
        -------
        ComputeCredentials
            The public HTTPS URL of the deployed Render service.
        """
        if not self._api_key or not self._owner_id:
            raise RuntimeError("Call login() before provision().")

        service_name = f"mf-{project_name.lower().replace('_', '-')}"[:63]

        env_vars = [
            {"key": "MF_METADATA_DB_DSN", "value": db.dsn},
            {"key": "MF_METADATA_DB_HOST", "value": db.host},
            {"key": "MF_METADATA_DB_PORT", "value": str(db.port)},
            {"key": "MF_METADATA_DB_USER", "value": db.username},
            {"key": "MF_METADATA_DB_PASS", "value": db.password},
            {"key": "MF_METADATA_DB_NAME", "value": db.database},
        ]

        async with httpx.AsyncClient(timeout=120.0) as client:
            existing_id = await self._find_existing_service(client, service_name)

            if existing_id:
                console.print(
                    f"[yellow]Updating existing Render service:[/yellow] "
                    f"{service_name!r} ({existing_id})"
                )
                # Update the existing service's environment variables.
                await client.put(
                    f"{_API_BASE}/services/{existing_id}/env-vars",
                    json=env_vars,
                    headers=self._headers(),
                )
                # Trigger a manual redeploy.
                deploy_response = await client.post(
                    f"{_API_BASE}/services/{existing_id}/deploys",
                    json={},
                    headers=self._headers(),
                )
                deploy_response.raise_for_status()
                service_id = existing_id
            else:
                console.print(f"[bold]Creating Render web service:[/bold] {service_name!r}")
                payload = {
                    "type": "web_service",
                    "name": service_name,
                    "ownerId": self._owner_id,
                    "serviceDetails": {
                        "env": "docker",
                        "dockerCommand": "",
                        "dockerContext": "",
                        "dockerfilePath": "",
                        "image": {
                            "ownerId": self._owner_id,
                            "registryCredentialId": None,
                            "imagePath": _METADATA_IMAGE,
                        },
                        "plan": "free",
                        "region": "oregon",
                        "numInstances": 1,
                        "envVars": env_vars,
                        "healthCheckPath": "/ping",
                    },
                }
                create_response = await client.post(
                    f"{_API_BASE}/services",
                    json=payload,
                    headers=self._headers(),
                )
                if create_response.status_code not in (200, 201):
                    raise RuntimeError(
                        f"Failed to create Render service "
                        f"(HTTP {create_response.status_code}):\n"
                        f"{create_response.text}"
                    )
                service_data = create_response.json()
                service_id = service_data["service"]["id"]

            console.print("[bold]Waiting for Render service to become live...[/bold]")
            service_url = await self._wait_for_service_url(client, service_id)

        console.print(f"[green]Render service deployed:[/green] {service_url}")
        return ComputeCredentials(service_url=service_url)

    async def _wait_for_service_url(
        self,
        client: httpx.AsyncClient,
        service_id: str,
        timeout_seconds: int = 300,
        poll_interval: float = 5.0,
    ) -> str:
        """
        Poll the Render API until the service has a URL and is live.

        Returns the service URL when ready.  Raises ``RuntimeError`` if the
        service does not become live within *timeout_seconds* seconds.
        """
        elapsed = 0.0
        while elapsed < timeout_seconds:
            response = await client.get(
                f"{_API_BASE}/services/{service_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            svc = response.json().get("service", {})
            url: str = svc.get("serviceDetails", {}).get("url", "")
            suspended: str = svc.get("suspended", "")

            if url and suspended != "suspended":
                return url

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise RuntimeError(
            f"Timed out waiting for Render service {service_id!r} to become live "
            f"(waited {timeout_seconds}s)."
        )
