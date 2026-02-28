"""
Local Metaflow UI proxy server.

Starts an aiohttp web server that:
  1. Serves the Metaflow UI static assets (downloaded on demand from the
     official Netflix/metaflow-ui GitHub releases).
  2. Proxies API calls from the UI to the configured Metaflow metadata
     service URL (read from ~/.metaflowconfig).
  3. Exposes a WebSocket endpoint at /ws that polls the metadata service
     every 3 seconds and pushes run/task state diffs to connected browsers.

Usage:
    mf-ui --port 8083
    # Then open http://localhost:8083 in your browser.
"""

from __future__ import annotations

import asyncio
import mimetypes
import shutil
import sys
import tarfile
import tempfile
import webbrowser
import zipfile
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web
from rich.console import Console
from rich.panel import Panel

from ..config import MetaflowConfig

console = Console()

# Directory where the UI static assets are cached.
_UI_CACHE_DIR = Path.home() / ".cache" / "metaflow-ephemeral" / "ui"

# GitHub repository for the Metaflow UI.
_UI_REPO = "Netflix/metaflow-ui"
_GITHUB_API = "https://api.github.com"

# Hop-by-hop headers that must not be forwarded.
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding",
    "upgrade", "host",
})


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_proxy(port: int = 8083) -> None:
    """
    Start the local Metaflow UI proxy on *port*.

    Reads the metadata service URL from ``~/.metaflowconfig`` and serves the
    Metaflow UI on ``http://localhost:<port>``.  API requests from the UI that
    start with ``/api/`` are forwarded to the upstream metadata service.  A
    WebSocket endpoint at ``/ws`` streams live run/task state diffs to
    connected browser clients.

    Parameters
    ----------
    port:
        Local TCP port to listen on (default: 8083).
    """
    config = MetaflowConfig()
    service_url = config.get_service_url()

    if not service_url:
        console.print(
            "[red]No Metaflow service URL configured.[/red]\n"
            "Run [bold]mf-setup[/bold] first to provision a metadata service."
        )
        sys.exit(1)

    # Normalise: strip trailing slash.
    service_url = service_url.rstrip("/")

    console.print(f"[cyan]Metadata service URL:[/cyan] {service_url}")

    # Ensure the UI assets are available locally.
    ui_dir = await _ensure_ui_assets()
    console.print(f"[cyan]Serving UI assets from:[/cyan] {ui_dir}")

    app = _build_app(service_url=service_url, ui_dir=ui_dir)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="127.0.0.1", port=port)
    await site.start()

    local_url = f"http://localhost:{port}"
    console.print(
        Panel(
            f"[bold green]Metaflow UI is running![/bold green]\n\n"
            f"Open [bold cyan]{local_url}[/bold cyan] in your browser.\n"
            f"Press [bold]Ctrl+C[/bold] to stop.",
            border_style="green",
            title="Metaflow UI Proxy",
        )
    )

    # Open the browser after a short delay so the server is ready.
    asyncio.ensure_future(_open_browser_after_delay(local_url, delay=1.0))

    try:
        # Run forever until interrupted.
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n[yellow]Shutting down UI proxy...[/yellow]")
    finally:
        await runner.cleanup()


async def _open_browser_after_delay(url: str, delay: float = 1.0) -> None:
    """Wait *delay* seconds then open *url* in the default browser."""
    await asyncio.sleep(delay)
    webbrowser.open(url)


# ---------------------------------------------------------------------------
# aiohttp application
# ---------------------------------------------------------------------------


def _build_app(service_url: str, ui_dir: Path) -> web.Application:
    """
    Construct the aiohttp Application with all routes configured.

    Routes:
        GET /ws               ->  WebSocket live-state stream
        ANY /api/{path:.*}    ->  proxy to upstream metadata service
        GET /{path:.*}        ->  serve static UI files (SPA fallback)
    """
    app = web.Application()

    # Store shared state in the app.
    app["service_url"] = service_url
    app["ui_dir"] = ui_dir

    # Lifecycle hooks.
    app.on_startup.append(_create_http_session)
    app.on_cleanup.append(_close_http_session)

    # Routes — order matters: more specific first.
    app.router.add_get("/ws", _ws_handler)
    app.router.add_route("*", "/api/{path_tail:.*}", _proxy_handler)
    app.router.add_get("/{path:.*}", _static_handler)

    return app


async def _create_http_session(app: web.Application) -> None:
    """Create a shared aiohttp ClientSession for proxying API calls."""
    connector = aiohttp.TCPConnector(ssl=False)
    app["session"] = aiohttp.ClientSession(connector=connector)


async def _close_http_session(app: web.Application) -> None:
    """Close the shared ClientSession gracefully."""
    session: aiohttp.ClientSession = app.get("session")
    if session and not session.closed:
        await session.close()


# ---------------------------------------------------------------------------
# Request handlers
# ---------------------------------------------------------------------------


async def _proxy_handler(request: web.Request) -> web.Response:
    """
    Forward ``/api/{path}`` requests to the upstream metadata service.

    The ``/api/`` prefix is stripped and the remainder of the path is
    appended to the configured service URL.  Request headers (except
    hop-by-hop headers and Host) and body are forwarded verbatim.
    Response headers and body are returned to the client with CORS headers
    added so the browser UI can communicate with the proxy.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]

    path_tail: str = request.match_info.get("path_tail", "")
    if not path_tail.startswith("/"):
        path_tail = "/" + path_tail
    upstream_url = service_url + path_tail

    # Forward query parameters.
    if request.query_string:
        upstream_url = f"{upstream_url}?{request.query_string}"

    forward_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }

    body = await request.read()

    try:
        async with session.request(
            method=request.method,
            url=upstream_url,
            headers=forward_headers,
            data=body or None,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as upstream_resp:
            resp_body = await upstream_resp.read()
            response_headers = {
                k: v for k, v in upstream_resp.headers.items()
                if k.lower() not in _HOP_BY_HOP
            }
            # Add CORS headers so the browser UI can communicate with the proxy.
            response_headers["Access-Control-Allow-Origin"] = "*"
            response_headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
            response_headers["Access-Control-Allow-Headers"] = "*"

            return web.Response(
                status=upstream_resp.status,
                headers=response_headers,
                body=resp_body,
            )
    except aiohttp.ClientError as exc:
        console.print(f"[red]Proxy error:[/red] {exc}")
        return web.Response(
            status=502,
            text=f"Bad Gateway: {exc}",
        )


async def _static_handler(request: web.Request) -> web.Response:
    """
    Serve static files from the cached Metaflow UI build directory.

    Falls back to serving ``index.html`` for any path that does not correspond
    to an existing file (SPA-style routing).
    """
    ui_dir: Path = request.app["ui_dir"]
    path_str: str = request.match_info.get("path", "")

    # Strip leading slash.
    if path_str.startswith("/"):
        path_str = path_str[1:]

    # Default to index.html for the root and unknown paths.
    if not path_str:
        path_str = "index.html"

    file_path = ui_dir / path_str

    # SPA fallback: if the file doesn't exist, serve index.html.
    if not file_path.exists() or not file_path.is_file():
        file_path = ui_dir / "index.html"

    if not file_path.exists():
        return web.Response(status=404, text="Metaflow UI assets not found.")

    content_type, _ = mimetypes.guess_type(str(file_path))
    if content_type is None:
        content_type = "application/octet-stream"

    return web.Response(
        body=file_path.read_bytes(),
        content_type=content_type,
    )


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    """
    WebSocket endpoint that streams live run/task state diffs to the browser.

    Polls the metadata service every 3 seconds for active runs (those with a
    heartbeat in the last 300 seconds).  When the state changes relative to
    the previous poll, the diff is pushed to the connected client as a JSON
    message of the form::

        {"type": "UPDATE", "data": {<run_id>: <run_state>, ...}}

    The connection is kept alive until the browser disconnects.
    """
    service_url: str = request.app["service_url"]

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    last_state: dict[str, Any] = {}

    async with aiohttp.ClientSession() as session:
        while not ws.closed:
            try:
                current = await _fetch_active_state(session, service_url)
                diff = _compute_diff(last_state, current)
                if diff:
                    await ws.send_json({"type": "UPDATE", "data": diff})
                    last_state = current
            except Exception:
                # Swallow errors silently; the client will retry on reconnect.
                pass
            await asyncio.sleep(3)

    return ws


async def _fetch_active_state(
    session: aiohttp.ClientSession,
    service_url: str,
) -> dict[str, Any]:
    """
    Fetch runs that have had a heartbeat in the last 300 seconds.

    Queries the metadata service's ``/flows`` and ``/runs`` endpoints to
    build a mapping of ``run_id -> run_info`` for currently active runs.

    Returns an empty dict if the service is unreachable or returns an error.
    """
    try:
        # Fetch all flows first.
        async with session.get(
            f"{service_url}/flows",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return {}
            flows_data: dict[str, Any] = await resp.json()

        flows = flows_data.get("data", flows_data) if isinstance(flows_data, dict) else flows_data
        if not isinstance(flows, list):
            return {}

        active_state: dict[str, Any] = {}

        for flow in flows:
            flow_id = flow.get("flow_id") or flow.get("id")
            if not flow_id:
                continue

            # Fetch runs for this flow.
            try:
                async with session.get(
                    f"{service_url}/flows/{flow_id}/runs",
                    params={"_order": "ts_epoch:desc", "_limit": "10"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as runs_resp:
                    if runs_resp.status != 200:
                        continue
                    runs_data: dict[str, Any] = await runs_resp.json()

                runs = runs_data.get("data", runs_data) if isinstance(runs_data, dict) else runs_data
                if not isinstance(runs, list):
                    continue

                for run in runs:
                    last_hb = run.get("last_heartbeat_ts") or run.get("ts_epoch", 0)
                    # Include runs with a heartbeat within the last 300 seconds.
                    import time
                    if isinstance(last_hb, (int, float)) and (time.time() - last_hb / 1000) < 300:
                        run_id = f"{flow_id}/{run.get('run_number') or run.get('run_id')}"
                        active_state[run_id] = run
            except Exception:
                continue

        return active_state

    except Exception:
        return {}


def _compute_diff(
    old: dict[str, Any],
    new: dict[str, Any],
) -> dict[str, Any]:
    """
    Return entries in *new* that differ from *old*.

    Includes new keys (added runs) and keys whose value has changed.
    Does not include keys that were removed from *new*.

    Returns an empty dict if there are no changes.
    """
    diff: dict[str, Any] = {}
    for key, new_val in new.items():
        if key not in old or old[key] != new_val:
            diff[key] = new_val
    return diff


# ---------------------------------------------------------------------------
# UI asset management
# ---------------------------------------------------------------------------


async def _ensure_ui_assets() -> Path:
    """
    Ensure the Metaflow UI static assets are present in the cache directory.

    Downloads the latest release from the Netflix/metaflow-ui GitHub
    repository if not already cached.  Returns the path to the directory
    containing ``index.html``.

    Returns
    -------
    Path
        Path to the directory containing the UI's ``index.html``.
    """
    index = _UI_CACHE_DIR / "index.html"
    if index.exists():
        console.print("[dim]Using cached UI assets.[/dim]")
        return _UI_CACHE_DIR

    console.print("[cyan]Downloading Metaflow UI assets...[/cyan]")
    _UI_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    release_info = await _fetch_latest_ui_release()
    download_url: str = release_info["download_url"]
    asset_name: str = release_info["name"]

    async with aiohttp.ClientSession() as session:
        async with session.get(
            download_url,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            resp.raise_for_status()
            data = await resp.read()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        archive_path = tmp_path / asset_name
        archive_path.write_bytes(data)

        # Extract the archive.
        if asset_name.endswith(".tar.gz") or asset_name.endswith(".tgz"):
            with tarfile.open(archive_path, "r:gz") as tf:
                tf.extractall(tmp_path)
        elif asset_name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(tmp_path)
        else:
            raise RuntimeError(
                f"Unexpected Metaflow UI archive format: {asset_name!r}"
            )

        # Find the index.html in the extracted files.
        index_candidates = list(tmp_path.rglob("index.html"))
        if not index_candidates:
            raise RuntimeError(
                "Could not find index.html in the downloaded Metaflow UI archive."
            )

        # Use the shallowest index.html (i.e. the build root).
        build_root = min(index_candidates, key=lambda p: len(p.parts)).parent

        # Copy all assets to the cache directory.
        for item in build_root.iterdir():
            dest = _UI_CACHE_DIR / item.name
            if item.is_dir():
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

    console.print(f"[green]UI assets cached to {_UI_CACHE_DIR}[/green]")
    return _UI_CACHE_DIR


async def _fetch_latest_ui_release() -> dict[str, str]:
    """
    Fetch the latest Metaflow UI release from GitHub and return asset info.

    Returns a dict with keys:
        - ``download_url``: URL to download the release archive.
        - ``name``: Asset filename.
        - ``tag``: Release tag name.
    """
    url = f"{_GITHUB_API}/repos/{_UI_REPO}/releases/latest"

    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status == 404:
                raise RuntimeError(
                    f"Metaflow UI GitHub repository {_UI_REPO!r} not found."
                )
            resp.raise_for_status()
            release: dict[str, Any] = await resp.json()

    tag: str = release.get("tag_name", "unknown")
    assets: list[dict[str, Any]] = release.get("assets", [])

    # Look for a .tar.gz or .zip build asset (not source code).
    build_asset = None
    for asset in assets:
        name: str = asset["name"].lower()
        if (
            ("build" in name or "dist" in name or "ui" in name)
            and (name.endswith(".tar.gz") or name.endswith(".zip") or name.endswith(".tgz"))
        ):
            build_asset = asset
            break

    # Fall back to the first .tar.gz or .zip asset.
    if build_asset is None:
        for asset in assets:
            name = asset["name"].lower()
            if name.endswith(".tar.gz") or name.endswith(".zip") or name.endswith(".tgz"):
                build_asset = asset
                break

    if build_asset is None:
        # Last resort: use the source tarball from the release.
        tarball_url: str = release.get("tarball_url", "")
        if tarball_url:
            return {
                "download_url": tarball_url,
                "name": f"metaflow-ui-{tag}.tar.gz",
                "tag": tag,
            }
        raise RuntimeError(
            f"No downloadable asset found in Metaflow UI release {tag!r}.\n"
            f"Available assets: {[a['name'] for a in assets]}"
        )

    return {
        "download_url": build_asset["browser_download_url"],
        "name": build_asset["name"],
        "tag": tag,
    }
