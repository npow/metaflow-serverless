"""
Handles reading/writing ~/.metaflowconfig (JSON file) and stack configuration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class StackConfig:
    """Represents the chosen provider stack for a Metaflow ephemeral service deployment."""

    compute: str   # "supabase" | "cloud-run" | "render"
    database: str  # "supabase" | "neon" | "cockroachdb"
    storage: str   # "supabase" | "r2" | "b2"

    def validate(self) -> None:
        """Validate that the stack combination is compatible."""
        from .providers.registry import COMPATIBLE_STACKS

        if self.compute not in COMPATIBLE_STACKS:
            raise ValueError(
                f"Unknown compute provider: {self.compute!r}. "
                f"Valid options: {list(COMPATIBLE_STACKS.keys())}"
            )

        compatible = COMPATIBLE_STACKS[self.compute]

        if self.database not in compatible["database"]:
            raise ValueError(
                f"Database provider {self.database!r} is not compatible with "
                f"compute provider {self.compute!r}. "
                f"Compatible databases: {compatible['database']}"
            )

        if self.storage not in compatible["storage"]:
            raise ValueError(
                f"Storage provider {self.storage!r} is not compatible with "
                f"compute provider {self.compute!r}. "
                f"Compatible storage: {compatible['storage']}"
            )


class MetaflowConfig:
    """
    Manages the ~/.metaflowconfig JSON configuration file.

    The config file stores Metaflow client settings including the metadata
    service URL, datastore configuration, and any provider-specific settings
    written by this tool.
    """

    DEFAULT_PATH = Path.home() / ".metaflowconfig"

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or self.DEFAULT_PATH

    def read(self) -> dict[str, Any]:
        """
        Read the existing Metaflow config file.

        Returns an empty dict if the file does not exist or contains invalid JSON.
        """
        if not self.path.exists():
            return {}

        try:
            text = self.path.read_text(encoding="utf-8").strip()
            if not text:
                return {}
            return json.loads(text)
        except json.JSONDecodeError:
            # Config file exists but is corrupt; return empty so we can overwrite it.
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """
        Merge *data* into the existing config and write back to disk.

        Existing keys not present in *data* are preserved.  Values in *data*
        take precedence over existing values for the same key.
        """
        current = self.read()
        current.update(data)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(current, indent=2) + "\n",
            encoding="utf-8",
        )

    def get_service_url(self) -> str | None:
        """
        Return the configured Metaflow metadata service URL, or None if not set.

        Checks the standard METAFLOW_SERVICE_URL key used by the Metaflow client.
        """
        config = self.read()
        return config.get("METAFLOW_SERVICE_URL") or None

    def get_datastore_config(self) -> dict[str, Any]:
        """
        Return the S3-compatible datastore configuration from the config file.

        Returns a dict that may contain any of the following keys (depending on
        what has been written by a prior setup run):

            METAFLOW_DATASTORE_SYSROOT_S3   - s3://bucket/prefix root path
            METAFLOW_S3_ENDPOINT_URL         - custom S3-compatible endpoint
            AWS_ACCESS_KEY_ID                - access key for the storage provider
            AWS_SECRET_ACCESS_KEY            - secret key for the storage provider
            AWS_DEFAULT_REGION               - region for the storage provider

        Returns an empty dict if none of these keys are present.
        """
        config = self.read()
        datastore_keys = {
            "METAFLOW_DATASTORE_SYSROOT_S3",
            "METAFLOW_S3_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
        }
        return {k: v for k, v in config.items() if k in datastore_keys}

    def set_service_url(self, url: str) -> None:
        """Convenience method to update only the metadata service URL."""
        self.write({"METAFLOW_SERVICE_URL": url})

    def set_datastore(
        self,
        *,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        bucket: str,
        region: str,
        prefix: str = "metaflow",
    ) -> None:
        """
        Write S3-compatible datastore settings into the Metaflow config.

        Parameters
        ----------
        endpoint_url:
            Full URL of the S3-compatible endpoint (e.g. the Cloudflare R2 or
            Backblaze B2 endpoint URL).
        access_key_id:
            S3 access key ID.
        secret_access_key:
            S3 secret access key.
        bucket:
            Bucket name.
        region:
            Region string (e.g. "auto" for Cloudflare R2).
        prefix:
            Path prefix inside the bucket (default: "metaflow").
        """
        sysroot = f"s3://{bucket}/{prefix}"
        self.write(
            {
                "METAFLOW_DEFAULT_DATASTORE": "s3",
                "METAFLOW_DATASTORE_SYSROOT_S3": sysroot,
                "METAFLOW_S3_ENDPOINT_URL": endpoint_url,
                "AWS_ACCESS_KEY_ID": access_key_id,
                "AWS_SECRET_ACCESS_KEY": secret_access_key,
                "AWS_DEFAULT_REGION": region,
            }
        )
