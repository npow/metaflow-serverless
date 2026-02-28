"""
Google Cloud Run compute provider.

Deploys the Metaflow metadata service as a serverless Cloud Run service.
Cloud Run's free tier includes 2 million requests/month, 360,000 GB-seconds
of memory, and 180,000 vCPU-seconds of compute time.

Authentication and deployment use the ``gcloud`` CLI.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from rich.console import Console

from ..installer import ensure_cli
from .base import ComputeCredentials, ComputeProvider, DatabaseCredentials

console = Console()

# Public Docker image for the Metaflow metadata service.
_METADATA_IMAGE = "netflixoss/metaflow-metadata-service:latest"


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


class CloudRunProvider(ComputeProvider):
    """
    Deploys the Metaflow metadata service to Google Cloud Run.

    Prerequisites:
        - A Google Cloud project (will attempt to use the currently active project).
        - The Cloud Run API enabled in the project.
        - The ``gcloud`` CLI authenticated with an account that has Cloud Run
          and Artifact Registry permissions.

    Free tier:
        - 2 million requests/month
        - 360,000 GB-seconds of memory/month
        - 180,000 vCPU-seconds/month
        - No egress charges for traffic to/from the internet (first 1 GB/month)

    Note: GCP requires a credit card to create a project, even for free-tier use.
    """

    name = "cloud-run"
    display_name = "Google Cloud Run (serverless containers, free tier)"
    requires_cc = True  # GCP requires a credit card, though free tier is generous.
    verification = "email+cc"
    cli_name = "gcloud"

    _DEFAULT_REGION = "us-central1"

    async def ensure_cli_installed(self) -> None:
        """Install the gcloud CLI if it is not already on the PATH."""
        await ensure_cli("gcloud")

    async def login(self) -> None:
        """
        Authenticate with Google Cloud via the gcloud CLI browser-based OAuth flow.

        Runs both ``gcloud auth login`` (for user credentials) and
        ``gcloud auth application-default login`` (for Application Default
        Credentials used by client libraries).
        """
        await self.ensure_cli_installed()
        console.print(
            "[bold]Opening browser for Google Cloud login...[/bold]"
        )

        # User account login.
        rc, stdout, stderr = await _run_async(
            ["gcloud", "auth", "login", "--brief"]
        )
        if rc != 0:
            raise RuntimeError(
                f"gcloud auth login failed (exit {rc}):\n{stderr.strip()}"
            )
        console.print("[green]Google Cloud login successful.[/green]")

        # Application Default Credentials (non-fatal if it fails).
        rc_adc, _, _ = await _run_async(
            ["gcloud", "auth", "application-default", "login"]
        )
        if rc_adc != 0:
            console.print(
                "[yellow]Warning: Application Default Credentials could not be "
                "configured; Cloud Run deployment may still succeed.[/yellow]"
            )

    async def _get_project_id(self) -> str:
        """Return the currently active GCP project ID."""
        rc, stdout, stderr = await _run_async(
            ["gcloud", "config", "get-value", "project"]
        )
        if rc != 0 or not stdout.strip():
            raise RuntimeError(
                "No GCP project configured. Run:\n"
                "  gcloud config set project YOUR_PROJECT_ID"
            )
        return stdout.strip()

    async def _enable_apis(self, project_id: str) -> None:
        """Enable the Cloud Run and Container Registry APIs for the project."""
        apis = [
            "run.googleapis.com",
            "containerregistry.googleapis.com",
        ]
        for api in apis:
            rc, _, _ = await _run_async(
                [
                    "gcloud", "services", "enable", api,
                    "--project", project_id,
                    "--quiet",
                ]
            )
            if rc != 0:
                # Non-fatal: the API might already be enabled.
                console.print(
                    f"[yellow]Note: could not enable {api!r} "
                    f"(may already be enabled).[/yellow]"
                )

    async def provision(
        self,
        db: DatabaseCredentials,
        project_name: str,
    ) -> ComputeCredentials:
        """
        Deploy the Metaflow metadata service container to Cloud Run.

        Enables required APIs, then creates or updates the Cloud Run service
        with database credentials passed as environment variables.

        Parameters
        ----------
        db:
            Database credentials to inject as environment variables.
        project_name:
            Used to derive the Cloud Run service name (max 49 chars).

        Returns
        -------
        ComputeCredentials
            The public HTTPS URL assigned by Cloud Run.
        """
        await self.ensure_cli_installed()

        project_id = await self._get_project_id()
        console.print(
            f"[bold]Enabling Cloud Run APIs for project[/bold] {project_id!r}..."
        )
        await self._enable_apis(project_id)

        # Sanitise service name: lowercase, hyphens only, max 49 chars.
        service_name = f"mf-{project_name.lower().replace('_', '-')}"[:49]
        console.print(
            f"[bold]Deploying Cloud Run service:[/bold] {service_name!r} "
            f"(image: {_METADATA_IMAGE})"
        )

        env_vars = ",".join(
            [
                f"MF_METADATA_DB_DSN={db.dsn}",
                f"MF_METADATA_DB_HOST={db.host}",
                f"MF_METADATA_DB_PORT={db.port}",
                f"MF_METADATA_DB_USER={db.username}",
                f"MF_METADATA_DB_PASS={db.password}",
                f"MF_METADATA_DB_NAME={db.database}",
            ]
        )

        rc, stdout, stderr = await _run_async(
            [
                "gcloud", "run", "deploy", service_name,
                "--image", _METADATA_IMAGE,
                "--platform", "managed",
                "--region", self._DEFAULT_REGION,
                "--project", project_id,
                "--allow-unauthenticated",
                "--port", "8080",
                "--set-env-vars", env_vars,
                "--format", "json",
                "--quiet",
            ]
        )
        if rc != 0:
            raise RuntimeError(
                f"Cloud Run deployment failed (exit {rc}):\n{stderr.strip()}"
            )

        # Parse the deployed service URL from the gcloud output.
        service_url: str = ""
        try:
            deploy_data = json.loads(stdout)
            service_url = deploy_data["status"]["url"]
        except (json.JSONDecodeError, KeyError):
            pass

        if not service_url:
            # Fall back to querying the service description.
            console.print(
                "[bold]Querying service URL...[/bold]"
            )
            rc2, stdout2, stderr2 = await _run_async(
                [
                    "gcloud", "run", "services", "describe", service_name,
                    "--platform", "managed",
                    "--region", self._DEFAULT_REGION,
                    "--project", project_id,
                    "--format", "json",
                ]
            )
            if rc2 != 0:
                raise RuntimeError(
                    f"Could not retrieve Cloud Run service URL (exit {rc2}):\n"
                    f"{stderr2.strip()}"
                )
            try:
                describe_data = json.loads(stdout2)
                service_url = describe_data["status"]["url"]
            except (json.JSONDecodeError, KeyError) as exc:
                raise RuntimeError(
                    f"Could not parse service URL from gcloud describe output:\n"
                    f"{stdout2}"
                ) from exc

        console.print(
            f"[green]Cloud Run service deployed:[/green] {service_url}"
        )
        return ComputeCredentials(service_url=service_url)
