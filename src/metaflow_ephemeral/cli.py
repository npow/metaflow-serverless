"""
CLI entry points for the metaflow-ephemeral-service package.

Provides two commands:
    mf-setup   - Interactive provisioning wizard
    mf-ui      - Local Metaflow UI proxy

These are registered as ``project.scripts`` in pyproject.toml:
    mf-setup = "metaflow_ephemeral.cli:setup_entrypoint"
    mf-ui    = "metaflow_ephemeral.cli:ui_entrypoint"
"""

from __future__ import annotations

import asyncio
import sys

import click
from rich.console import Console

console = Console()


@click.group()
def cli() -> None:
    """Metaflow Ephemeral Service CLI."""
    pass


@cli.command("setup")
def setup() -> None:
    """Provision a free Metaflow metadata service using serverless providers.

    Guides you through selecting a stack (compute, database, storage),
    authenticating with each provider, and writing the resulting configuration
    to ~/.metaflowconfig.

    Supported stacks:

    \b
      Compute:  Supabase | Google Cloud Run | Render
      Database: Supabase | Neon | CockroachDB
      Storage:  Supabase | Cloudflare R2 | Backblaze B2
    """
    from .setup.wizard import SetupWizard

    try:
        asyncio.run(SetupWizard().run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Setup interrupted.[/yellow]")
        sys.exit(0)
    except Exception as exc:
        console.print(f"\n[red]Setup failed:[/red] {exc}")
        if "--debug" in sys.argv:
            raise
        sys.exit(1)


@cli.command("ui")
@click.option(
    "--port",
    default=8083,
    show_default=True,
    type=int,
    help="Local port to serve the Metaflow UI on.",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Delete the cached UI assets and re-download them.",
)
def ui(port: int, clear_cache: bool) -> None:
    """Start a local Metaflow UI proxy.

    Reads the metadata service URL from ~/.metaflowconfig and serves the
    Metaflow UI on http://localhost:<port>.  API calls from the UI are
    forwarded to the upstream metadata service.

    Run mf-setup first to provision a metadata service and write the config.
    """
    if clear_cache:
        _clear_ui_cache()

    from .ui_proxy.proxy import run_proxy

    try:
        asyncio.run(run_proxy(port=port))
    except KeyboardInterrupt:
        console.print("\n[yellow]UI proxy stopped.[/yellow]")
        sys.exit(0)
    except Exception as exc:
        console.print(f"\n[red]UI proxy error:[/red] {exc}")
        if "--debug" in sys.argv:
            raise
        sys.exit(1)


def _clear_ui_cache() -> None:
    """Remove the cached Metaflow UI assets so they will be re-downloaded."""
    import shutil
    from pathlib import Path

    cache_dir = Path.home() / ".cache" / "metaflow-ephemeral" / "ui"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        console.print(f"[yellow]Cleared UI asset cache:[/yellow] {cache_dir}")
    else:
        console.print("[dim]UI asset cache is already empty.[/dim]")


# ---------------------------------------------------------------------------
# Entry point functions referenced by pyproject.toml [project.scripts]
# ---------------------------------------------------------------------------


def setup_entrypoint() -> None:
    """
    Entry point for the ``mf-setup`` console script.

    Invokes the ``setup`` subcommand directly, so users can run ``mf-setup``
    without typing ``mf-setup setup``.
    """
    cli(["setup"], standalone_mode=True)


def ui_entrypoint() -> None:
    """
    Entry point for the ``mf-ui`` console script.

    Invokes the ``ui`` subcommand directly, so users can run ``mf-ui``
    (and ``mf-ui --port 8080``) without typing ``mf-ui ui``.

    All CLI arguments passed to the process are forwarded to the ``ui``
    subcommand, allowing flags like ``--port`` and ``--clear-cache`` to work.
    """
    # sys.argv[0] is the script name; pass the rest as arguments to the ui cmd.
    args = ["ui"] + sys.argv[1:]
    cli(args, standalone_mode=True)


if __name__ == "__main__":
    cli()
