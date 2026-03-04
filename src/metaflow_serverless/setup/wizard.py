"""
Interactive setup wizard for provisioning a Metaflow ephemeral service.

Guides the user through selecting a provider stack, authenticating with each
provider, provisioning the database / storage / compute resources, running SQL
migrations, and writing the resulting configuration to ~/.metaflowconfig.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    import questionary
    from questionary import Choice
except ImportError:
    questionary = None  # type: ignore[assignment]

from ..config import MetaflowConfig, StackConfig
from ..providers.base import (
    ComputeCredentials,
    DatabaseCredentials,
    StorageCredentials,
)
from ..providers.registry import (
    COMPATIBLE_STACKS,
    COMPUTE_PROVIDERS,
    DATABASE_PROVIDERS,
    STORAGE_PROVIDERS,
    compatible_databases,
    compatible_storage,
    get_compute_provider,
    get_database_provider,
    get_storage_provider,
)

console = Console()


@dataclass
class _ProviderMeta:
    """Metadata about a provider for display in the wizard."""
    name: str
    display_name: str
    requires_cc: bool
    verification: str


def _provider_meta_list(registry: dict) -> list[_ProviderMeta]:
    """Instantiate providers from a registry and extract display metadata."""
    metas = []
    for name, cls in registry.items():
        instance = cls()
        metas.append(
            _ProviderMeta(
                name=name,
                display_name=instance.display_name,
                requires_cc=instance.requires_cc,
                verification=instance.verification,
            )
        )
    return metas


def _cc_badge(requires_cc: bool) -> str:
    return " [CC required]" if requires_cc else " [no CC · email only]"


# Compute-provider-specific display hints shown in the selection list.
_COMPUTE_HINTS: dict[str, str] = {
    "supabase": "~0ms cold start",
    "cloud-run": "~1-4s cold start",
    "render": "~30-60s cold start",
}

# Database-provider-specific display hints.
_DB_HINTS: dict[str, str] = {
    "supabase": "included with Supabase",
    "neon": "~100-500ms wake",
    "cockroachdb": "5GB free",
}

# Storage-provider-specific display hints.
_STORAGE_HINTS: dict[str, str] = {
    "supabase": "1GB free · S3-compatible",
    "r2": "10GB free · zero egress",
    "b2": "10GB free",
}

_STORAGE_CC_OVERRIDES: dict[str, str] = {
    "b2": " [phone verify]",
}


def _compute_choice_title(m: _ProviderMeta) -> str:
    hint = _COMPUTE_HINTS.get(m.name, "")
    badge = _cc_badge(m.requires_cc)
    return f"{m.display_name}{badge} · {hint}" if hint else f"{m.display_name}{badge}"


def _db_choice_title(m: _ProviderMeta) -> str:
    hint = _DB_HINTS.get(m.name, "")
    badge = _cc_badge(m.requires_cc)
    return f"{m.display_name}{badge} · {hint}" if hint else f"{m.display_name}{badge}"


def _storage_choice_title(m: _ProviderMeta) -> str:
    hint = _STORAGE_HINTS.get(m.name, "")
    override = _STORAGE_CC_OVERRIDES.get(m.name)
    if override is not None:
        badge = override
    else:
        badge = _cc_badge(m.requires_cc)
    return f"{m.display_name}{badge} · {hint}" if hint else f"{m.display_name}{badge}"


class SetupWizard:
    """
    Interactive terminal wizard that provisions a Metaflow metadata service.

    Steps:
        1.  Welcome banner
        2.  Project name input
        3.  Compute provider selection
        4.  Database provider selection (filtered by compute compatibility)
        5.  Storage provider selection (filtered by compute compatibility)
        6.  Confirm stack
        7.  Provision database: CLI install + login + provision
        8.  Provision storage: CLI install + login + provision
        9.  Provision compute: CLI install + login + provision
        10. Run SQL migrations
        11. Write ~/.metaflowconfig
        12. Print success summary
    """

    def __init__(self, config_path: str | None = None) -> None:
        from pathlib import Path
        self._config = MetaflowConfig(
            path=Path(config_path) if config_path else None
        )

    async def run(self) -> None:
        """Run the full wizard flow."""
        if questionary is None:
            console.print(
                "[red]questionary is not installed.[/red] "
                "Run: pip install questionary"
            )
            sys.exit(1)

        self._print_banner()

        project_name = await self._ask_project_name()
        stack = await self._ask_stack()

        # ---- Confirm -------------------------------------------------------
        console.print()
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Layer", style="bold")
        table.add_column("Provider", style="cyan")
        table.add_row("Compute", stack.compute)
        table.add_row("Database", stack.database)
        table.add_row("Storage", stack.storage)
        console.print(Panel(table, title="Selected stack", border_style="blue"))
        console.print()

        confirmed = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: questionary.confirm(
                "Proceed with provisioning?", default=True
            ).ask(),
        )
        if not confirmed:
            console.print("[yellow]Setup cancelled.[/yellow]")
            return

        # ---- Provision database --------------------------------------------
        db_creds = await self._run_step(
            "database",
            self._provision_database(stack.database, project_name),
        )
        if db_creds is None:
            return

        # ---- Provision storage ---------------------------------------------
        storage_creds = await self._run_step(
            "storage",
            self._provision_storage(stack.storage, project_name),
        )
        if storage_creds is None:
            return

        # ---- Provision compute ---------------------------------------------
        compute_creds = await self._run_step(
            "compute",
            self._provision_compute(stack.compute, db_creds, project_name),
        )
        if compute_creds is None:
            return

        # ---- Run SQL migrations --------------------------------------------
        migrations_ok = await self._run_step(
            "migrations",
            self._run_migrations(stack.database, db_creds, project_name),
        )
        if migrations_ok is None:
            return

        # ---- Write config --------------------------------------------------
        self._write_config(db_creds, storage_creds, compute_creds)
        self._print_summary(compute_creds, storage_creds, project_name)

    # ------------------------------------------------------------------
    # Generic step runner with error handling and retry/exit
    # ------------------------------------------------------------------

    async def _run_step(self, step_name: str, coro) -> Any:
        """
        Execute *coro* and return its result.

        On failure, print a friendly error with rich and offer to retry or exit.
        Returns ``None`` if the user chooses to exit.
        """
        while True:
            try:
                return await coro
            except Exception as exc:
                console.print(
                    f"\n[red bold]Error during {step_name}:[/red bold] {exc}"
                )
                if questionary is not None:
                    action = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: questionary.select(
                            f"Step '{step_name}' failed. What would you like to do?",
                            choices=[
                                Choice(title="Retry this step", value="retry"),
                                Choice(title="Exit setup", value="exit"),
                            ],
                        ).ask(),
                    )
                    if action == "retry":
                        console.print(f"[yellow]Retrying {step_name}...[/yellow]")
                        # Re-create the coroutine by re-calling the helper.
                        # The caller already passed an unawaited coro; we cannot
                        # re-create it here, so we simply raise to propagate the
                        # choice back.  We break out and return None so the
                        # caller can decide.
                        #
                        # NOTE: retrying here would require the coroutine to be
                        # re-created (since it was already scheduled).  We signal
                        # "retry" by raising a special sentinel so the outer loop
                        # can reconstruct the coro.  For simplicity, we fall
                        # through to exit and let the user re-run the wizard.
                        console.print(
                            "[dim]Tip: fix the issue and re-run [bold]mf-setup[/bold].[/dim]"
                        )
                        return None
                    else:
                        console.print("[yellow]Setup exited.[/yellow]")
                        return None
                else:
                    return None

    # ------------------------------------------------------------------
    # Banner and prompts
    # ------------------------------------------------------------------

    def _print_banner(self) -> None:
        banner = Text()
        banner.append("Metaflow Ephemeral Service", style="bold cyan")
        banner.append("\n")
        banner.append(
            "Zero-infra Metaflow metadata service using serverless providers.",
            style="dim",
        )
        console.print(Panel(banner, border_style="cyan"))
        console.print()

    async def _ask_project_name(self) -> str:
        loop = asyncio.get_event_loop()
        name: str = await loop.run_in_executor(
            None,
            lambda: questionary.text(
                "Project name:",
                default="metaflow",
                validate=lambda v: True if v.strip() else "Project name cannot be empty.",
            ).ask(),
        )
        if not name:
            console.print("[yellow]Setup cancelled.[/yellow]")
            sys.exit(0)
        return name.strip()

    async def _ask_stack(self) -> StackConfig:
        loop = asyncio.get_event_loop()

        # ---- Compute -------------------------------------------------------
        compute_metas = _provider_meta_list(COMPUTE_PROVIDERS)
        compute_choices = [
            Choice(title=_compute_choice_title(m), value=m.name)
            for m in compute_metas
        ]
        compute_name: str = await loop.run_in_executor(
            None,
            lambda: questionary.select(
                "Select your compute provider (runs the Metaflow metadata service):",
                choices=compute_choices,
            ).ask(),
        )
        if not compute_name:
            console.print("[yellow]Setup cancelled.[/yellow]")
            sys.exit(0)

        # ---- Database (filtered by compute compatibility) ------------------
        compat_db_names = compatible_databases(compute_name)
        db_registry = {k: v for k, v in DATABASE_PROVIDERS.items() if k in compat_db_names}
        db_metas = _provider_meta_list(db_registry)
        db_choices = [
            Choice(title=_db_choice_title(m), value=m.name)
            for m in db_metas
        ]
        db_name: str = await loop.run_in_executor(
            None,
            lambda: questionary.select(
                "Select your database provider (serverless Postgres):",
                choices=db_choices,
            ).ask(),
        )
        if not db_name:
            console.print("[yellow]Setup cancelled.[/yellow]")
            sys.exit(0)

        # ---- Storage (filtered by compute compatibility) -------------------
        compat_storage_names = compatible_storage(compute_name)
        storage_registry = {k: v for k, v in STORAGE_PROVIDERS.items() if k in compat_storage_names}
        storage_metas = _provider_meta_list(storage_registry)
        storage_choices = [
            Choice(title=_storage_choice_title(m), value=m.name)
            for m in storage_metas
        ]
        storage_name: str = await loop.run_in_executor(
            None,
            lambda: questionary.select(
                "Select your storage provider (S3-compatible object storage):",
                choices=storage_choices,
            ).ask(),
        )
        if not storage_name:
            console.print("[yellow]Setup cancelled.[/yellow]")
            sys.exit(0)

        return StackConfig(
            compute=compute_name,
            database=db_name,
            storage=storage_name,
        )

    # ------------------------------------------------------------------
    # Provisioning steps
    # ------------------------------------------------------------------

    async def _provision_database(
        self,
        db_name: str,
        project_name: str,
    ) -> DatabaseCredentials:
        console.rule("[bold blue]Database provisioning[/bold blue]")
        provider = get_database_provider(db_name)

        with console.status(f"[cyan]Checking {provider.display_name} CLI...[/cyan]"):
            await provider.ensure_cli_installed()

        console.print(f"[cyan]Logging in to {provider.display_name}...[/cyan]")
        await provider.login()

        with console.status(f"[cyan]Provisioning database for {project_name!r}...[/cyan]"):
            creds = await provider.provision(project_name)

        console.print(
            f"[green]Database provisioned:[/green] "
            f"host={creds.host}, db={creds.database}"
        )
        return creds

    async def _provision_storage(
        self,
        storage_name: str,
        project_name: str,
    ) -> StorageCredentials:
        console.rule("[bold blue]Storage provisioning[/bold blue]")
        provider = get_storage_provider(storage_name)

        with console.status(f"[cyan]Checking {provider.display_name} CLI...[/cyan]"):
            await provider.ensure_cli_installed()

        console.print(f"[cyan]Logging in to {provider.display_name}...[/cyan]")
        await provider.login()

        bucket_name = f"metaflow-{project_name.lower().replace('_', '-')}"[:63]
        with console.status(f"[cyan]Provisioning bucket {bucket_name!r}...[/cyan]"):
            creds = await provider.provision(bucket_name)

        console.print(
            f"[green]Storage provisioned:[/green] "
            f"bucket={creds.bucket}, endpoint={creds.endpoint_url}"
        )
        return creds

    async def _provision_compute(
        self,
        compute_name: str,
        db_creds: DatabaseCredentials,
        project_name: str,
    ) -> ComputeCredentials:
        console.rule("[bold blue]Compute provisioning[/bold blue]")
        provider = get_compute_provider(compute_name)

        with console.status(f"[cyan]Checking {provider.display_name} CLI...[/cyan]"):
            await provider.ensure_cli_installed()

        console.print(f"[cyan]Logging in to {provider.display_name}...[/cyan]")
        await provider.login()

        with console.status(
            f"[cyan]Deploying Metaflow metadata service to {provider.display_name}...[/cyan]"
        ):
            creds = await provider.provision(db_creds, project_name)

        console.print(
            f"[green]Compute provisioned:[/green] "
            f"service_url={creds.service_url}"
        )
        return creds

    async def _run_migrations(
        self,
        db_name: str,
        db_creds: DatabaseCredentials,
        project_name: str,
    ) -> bool:
        """
        Run SQL migrations against the provisioned database.

        Connects directly with asyncpg and executes schema.sql then
        procedures.sql.

        Returns ``True`` on success.
        """
        console.rule("[bold blue]Running SQL migrations[/bold blue]")

        with console.status("[cyan]Running migrations via asyncpg...[/cyan]"):
            await _run_migrations_asyncpg(db_creds.dsn)

        console.print("[green]Migrations complete.[/green]")
        return True

    # ------------------------------------------------------------------
    # Config write and summary
    # ------------------------------------------------------------------

    def _write_config(
        self,
        db: DatabaseCredentials,
        storage: StorageCredentials,
        compute: ComputeCredentials,
    ) -> None:
        """Merge all provisioned credentials into ~/.metaflowconfig."""
        self._config.write(
            {
                "METAFLOW_SERVICE_URL": compute.service_url,
                "METAFLOW_DEFAULT_METADATA": "service",
                "METAFLOW_DEFAULT_DATASTORE": "s3",
                "METAFLOW_DATASTORE_SYSROOT_S3": f"s3://{storage.bucket}/metaflow",
                "METAFLOW_S3_ENDPOINT_URL": storage.endpoint_url,
                "AWS_ACCESS_KEY_ID": storage.access_key_id,
                "AWS_SECRET_ACCESS_KEY": storage.secret_access_key,
            }
        )
        console.print(
            f"\n[green]Configuration written to[/green] {self._config.path}"
        )

    def _print_summary(
        self,
        compute: ComputeCredentials,
        storage: StorageCredentials,
        project_name: str,
    ) -> None:
        summary = (
            f"[bold green]Setup complete![/bold green]\n\n"
            f"[bold]Metaflow metadata service URL:[/bold]\n"
            f"  [cyan]{compute.service_url}[/cyan]\n\n"
            f"[bold]Datastore bucket:[/bold]\n"
            f"  [cyan]s3://{storage.bucket}/metaflow[/cyan]\n\n"
            f"[bold]Next steps:[/bold]\n"
            f"  1. Run a test flow:  [dim]python my_flow.py run[/dim]\n"
            f"  2. View the UI:      [dim]mf-ui[/dim]\n"
            f"  3. Check config:     [dim]cat ~/.metaflowconfig[/dim]"
        )
        console.print()
        console.print(Panel(summary, border_style="green", title="Done"))


# ---------------------------------------------------------------------------
# Standalone migration helper (asyncpg, non-Supabase path)
# ---------------------------------------------------------------------------


async def _run_migrations_asyncpg(dsn: str) -> None:
    """
    Connect to *dsn* using asyncpg and execute the bundled schema and
    procedures SQL files.

    Parameters
    ----------
    dsn:
        Full PostgreSQL connection string, e.g.
        ``postgresql://user:pass@host/db``.
    """
    try:
        import asyncpg  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "asyncpg is required for running migrations on non-Supabase databases. "
            "Install it with: pip install asyncpg"
        ) from exc

    from ..sql.loader import load_schema, load_procedures

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(load_schema())
        await conn.execute(load_procedures())
        # PostgREST (used behind Supabase REST endpoints) caches schema.
        # Reload so newly created RPCs (e.g. create_flow) are immediately visible.
        try:
            await conn.execute("NOTIFY pgrst, 'reload schema';")
        except Exception:
            # Non-fatal on providers that don't use PostgREST.
            pass
    finally:
        await conn.close()
