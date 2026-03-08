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
import contextlib
import json
import mimetypes
import os
import re
import shutil
import sys
import tarfile
import tempfile
import time
import webbrowser
import zipfile
from functools import lru_cache
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
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
    }
)
_RUNS_PATH_RE = re.compile(r"^/flows/[^/]+/runs(?:/[^/]+)?$")


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
    service_auth_key = config.get_service_auth_key()
    datastore_config = config.get_datastore_config()

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

    app = _build_app(
        service_url=service_url,
        ui_dir=ui_dir,
        service_auth_key=service_auth_key,
        datastore_config=datastore_config,
    )

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


def _build_app(
    service_url: str,
    ui_dir: Path,
    service_auth_key: str | None = None,
    datastore_config: dict[str, Any] | None = None,
) -> web.Application:
    """
    Construct the aiohttp Application with all routes configured.

    Routes:
        GET /ws               ->  WebSocket live-state stream
        ANY /api/{path:.*}    ->  proxy to upstream metadata service
        GET /{path:.*}        ->  serve static UI files (SPA fallback)
    """

    @web.middleware
    async def _log_middleware(request: web.Request, handler):
        resp = await handler(request)
        if request.path.startswith("/api"):
            console.print(f"[dim]{request.method} {request.path_qs} → {resp.status}[/dim]")
        return resp

    app = web.Application(middlewares=[_log_middleware])

    # Store shared state in the app.
    app["service_url"] = service_url
    app["ui_dir"] = ui_dir
    app["service_auth_key"] = service_auth_key
    app["datastore_config"] = datastore_config or {}

    # Lifecycle hooks.
    app.on_startup.append(_create_http_session)
    app.on_cleanup.append(_close_http_session)

    # Routes — order matters: more specific first.
    app.router.add_get("/ws", _ws_handler)
    app.router.add_get("/api/ws", _ws_handler)
    app.router.add_get("/api/runs", _runs_compat_handler)
    app.router.add_get("/api/flows/{flow_id}/runs/autocomplete", _runs_autocomplete_handler)
    app.router.add_get("/api/flows/{flow_id}/runs/{run_id}/parameters", _run_parameters_handler)
    app.router.add_get("/api/flows/{flow_id}/runs/{run_id}/tasks", _run_tasks_handler)
    app.router.add_get("/api/flows/{flow_id}/runs/{run_id}/dag", _run_dag_handler)
    app.router.add_get(
        "/api/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/attempts",
        _task_attempts_handler,
    )
    app.router.add_get(
        "/api/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_id}",
        _task_detail_handler,
    )
    app.router.add_get(
        "/api/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata",
        _task_metadata_handler,
    )
    app.router.add_get(
        "/api/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/logs/{stream}",
        _task_logs_handler,
    )
    app.router.add_get("/api/features", _features_handler)
    app.router.add_get("/api/plugin", _plugin_handler)
    app.router.add_get("/api/notifications", _notifications_handler)
    app.router.add_get("/api/links", _links_handler)
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
    service_auth_key: str | None = request.app.get("service_auth_key")

    path_tail: str = request.match_info.get("path_tail", "")
    if not path_tail.startswith("/"):
        path_tail = "/" + path_tail
    upstream_url = service_url + path_tail

    # Forward query parameters.
    if request.query_string:
        upstream_url = f"{upstream_url}?{request.query_string}"

    forward_headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP}
    if service_auth_key:
        forward_headers["x-api-key"] = service_auth_key

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
            path_for_norm = path_tail.split("?", 1)[0]
            if upstream_resp.status == 200 and _RUNS_PATH_RE.match(path_for_norm):
                resp_body = _normalize_runs_payload(resp_body)
                resp_body = _wrap_data_payload(resp_body)
            if upstream_resp.status == 200 and (
                path_for_norm.endswith("/metadata")
                or path_for_norm.endswith("/artifacts")
                or path_for_norm.endswith("/steps")
                or path_for_norm.endswith("/tasks")
            ):
                resp_body = _wrap_data_payload(resp_body)
            response_headers = {
                k: v for k, v in upstream_resp.headers.items() if k.lower() not in _HOP_BY_HOP
            }
            # aiohttp may decode gzip/brotli transparently; avoid mismatched
            # content-encoding/content-length on forwarded bodies.
            response_headers.pop("Content-Encoding", None)
            response_headers.pop("Content-Length", None)
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


async def _features_handler(_request: web.Request) -> web.Response:
    return web.json_response({})


async def _plugin_handler(_request: web.Request) -> web.Response:
    return web.json_response([])


async def _notifications_handler(_request: web.Request) -> web.Response:
    return web.json_response([])


async def _links_handler(_request: web.Request) -> web.Response:
    return web.json_response([])


async def _runs_compat_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for Metaflow UI's global /api/runs call.

    Aggregates runs across all flows from the metadata service and returns them
    sorted by ts_epoch descending.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None

    runs: list[dict[str, Any]] = []
    try:
        async with session.get(
            f"{service_url}/flows",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return web.json_response([])
            flows_data: Any = await resp.json()
    except Exception:
        return web.json_response([])

    flows = flows_data.get("data", flows_data) if isinstance(flows_data, dict) else flows_data
    if not isinstance(flows, list):
        return web.json_response([])

    for flow in flows:
        flow_id = flow.get("flow_id") or flow.get("id")
        if not flow_id:
            continue
        try:
            async with session.get(
                f"{service_url}/flows/{flow_id}/runs",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    continue
                data: Any = await resp.json()
            flow_runs = data.get("data", data) if isinstance(data, dict) else data
            if isinstance(flow_runs, list):
                for run in flow_runs:
                    if isinstance(run, dict):
                        if run.get("run_id") is None and run.get("run_number") is not None:
                            run["run_id"] = str(run.get("run_number"))
                        if "user" not in run and run.get("user_name") is not None:
                            run["user"] = run.get("user_name")
                        if "status" not in run:
                            run["status"] = _infer_run_status(run)
                runs.extend(flow_runs)
        except Exception:
            continue

    runs.sort(key=lambda r: _safe_ts(r.get("ts_epoch")), reverse=True)

    # Respect common pagination controls used by the UI.
    limit_param = request.query.get("_limit")
    page_param = request.query.get("_page")
    try:
        limit = int(limit_param) if limit_param else 30
    except ValueError:
        limit = 30
    try:
        page = int(page_param) if page_param else 1
    except ValueError:
        page = 1
    page = max(page, 1)
    limit = max(limit, 1)
    start = (page - 1) * limit
    end = start + limit

    payload = {
        "data": runs[start:end],
        "total_count": len(runs),
        "time": int(time.time() * 1000),
    }
    return web.json_response(payload)


async def _runs_autocomplete_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for /api/flows/<flow>/runs/autocomplete.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")

    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return web.json_response([])
            data: Any = await resp.json()
    except Exception:
        return web.json_response([])

    runs = data.get("data", data) if isinstance(data, dict) else data
    if not isinstance(runs, list):
        return web.json_response([])

    needle = request.query.get("run:co") or request.query.get("run") or request.query.get("q") or ""
    needle = str(needle).strip()

    out: list[dict[str, Any]] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        run_number = run.get("run_number")
        run_id = run.get("run_id")
        run_id_value = (
            str(run_id)
            if run_id is not None
            else (str(run_number) if run_number is not None else "")
        )
        if needle and needle not in run_id_value:
            continue
        if run.get("run_id") is None and run_number is not None:
            run = dict(run)
            run["run_id"] = str(run_number)
        if "user" not in run and run.get("user_name") is not None:
            run["user"] = run.get("user_name")
        if "status" not in run:
            run["status"] = _infer_run_status(run)
        out.append(run)

    out.sort(key=lambda r: _safe_ts(r.get("ts_epoch")), reverse=True)
    limit = request.query.get("_limit")
    try:
        n = int(limit) if limit else 20
    except ValueError:
        n = 20
    return web.json_response(out[: max(n, 1)])


async def _run_parameters_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for /api/flows/<flow>/runs/<run>/parameters.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")

    params: dict[str, Any] = {}
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/metadata",
            params={"step_name": "_parameters"},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                data: Any = await resp.json()
                entries = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(entries, list):
                    for item in entries:
                        if not isinstance(item, dict):
                            continue
                        field = item.get("field_name")
                        value = item.get("value")
                        if isinstance(field, str) and field.startswith("parameter_"):
                            params[field] = {"value": value}
    except Exception:
        pass

    return web.json_response({"data": params, "links": {"next": None}})


async def _run_tasks_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for /api/flows/<flow>/runs/<run>/tasks.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")

    # Fetch run-level metadata once to extract attempt-done timestamps for the timeline.
    task_finished_at: dict[int, int] = {}
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/metadata",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                raw: Any = await resp.json()
                entries = raw if isinstance(raw, list) else raw.get("data", [])
                if isinstance(entries, list):
                    for entry in entries:
                        if entry.get("field_name") == "attempt-done":
                            tid = entry.get("task_id")
                            ts = entry.get("ts_epoch")
                            if tid is not None and ts is not None:
                                task_finished_at[int(tid)] = int(ts)
    except Exception:
        pass

    all_tasks: list[dict[str, Any]] = []
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return web.json_response({"data": [], "links": {"next": None}})
            steps_data: Any = await resp.json()
    except Exception:
        return web.json_response({"data": [], "links": {"next": None}})

    steps = steps_data.get("data", steps_data) if isinstance(steps_data, dict) else steps_data
    if not isinstance(steps, list):
        return web.json_response({"data": [], "links": {"next": None}})

    for step in steps:
        if not isinstance(step, dict):
            continue
        step_name = step.get("step_name")
        if not step_name:
            continue
        try:
            async with session.get(
                f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    continue
                tasks_data: Any = await resp.json()
            tasks = (
                tasks_data.get("data", tasks_data) if isinstance(tasks_data, dict) else tasks_data
            )
            if isinstance(tasks, list):
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    enriched = dict(task)
                    if enriched.get("run_id") is None:
                        enriched["run_id"] = str(run_id)
                    if "user" not in enriched and enriched.get("user_name") is not None:
                        enriched["user"] = enriched.get("user_name")
                    if "status" not in enriched or not enriched.get("status"):
                        enriched["status"] = await _infer_task_status(
                            session=session,
                            service_url=service_url,
                            headers=headers,
                            flow_id=flow_id,
                            run_id=str(run_id),
                            step_name=str(step_name),
                            task=enriched,
                        )
                    # Populate timeline fields.
                    started_at = enriched.get("ts_epoch")
                    task_id_int = enriched.get("task_id")
                    finished_at = (
                        task_finished_at.get(task_id_int) if task_id_int is not None else None
                    )
                    if "started_at" not in enriched:
                        enriched["started_at"] = started_at
                    if "finished_at" not in enriched:
                        enriched["finished_at"] = finished_at
                    if "duration" not in enriched:
                        s = enriched.get("started_at")
                        f = enriched.get("finished_at")
                        enriched["duration"] = (f - s) if (s and f) else None
                    if "attempt_id" not in enriched:
                        enriched["attempt_id"] = 0
                    all_tasks.append(enriched)
        except Exception:
            continue

    all_tasks.sort(key=lambda t: _safe_ts(t.get("ts_epoch")))
    return web.json_response({"data": all_tasks, "links": {"next": None}})


async def _run_dag_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for /api/flows/<flow>/runs/<run>/dag.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")

    # First try native endpoint if upstream supports it.
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/dag",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                data: Any = await resp.json()
                dag = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(dag, dict) and dag:
                    return web.json_response({"data": dag, "links": {"next": None}})
    except Exception:
        pass

    # Synthesize a minimal DAG from step names if /dag is unavailable.
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return web.json_response({"data": {}, "links": {"next": None}})
            steps_data: Any = await resp.json()
    except Exception:
        return web.json_response({"data": {}, "links": {"next": None}})

    steps = steps_data.get("data", steps_data) if isinstance(steps_data, dict) else steps_data
    if not isinstance(steps, list):
        return web.json_response({"data": {}, "links": {"next": None}})

    logical_steps = [
        s
        for s in steps
        if isinstance(s, dict) and s.get("step_name") and s.get("step_name") != "_parameters"
    ]
    logical_steps.sort(key=lambda s: _safe_ts(s.get("ts_epoch")))
    ordered_names = [str(s["step_name"]) for s in logical_steps]
    if not ordered_names:
        return web.json_response({"data": {}, "links": {"next": None}})

    # Fetch task counts per step in parallel to infer foreach/join step types.
    async def _count_tasks_for_step(step_name: str) -> int:
        try:
            async with session.get(
                f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return 1
                data: Any = await resp.json()
            tasks = data.get("data", data) if isinstance(data, dict) else data
            return len(tasks) if isinstance(tasks, list) else 1
        except Exception:
            return 1

    counts_list = await asyncio.gather(*(_count_tasks_for_step(n) for n in ordered_names))
    task_counts: dict[str, int] = dict(zip(ordered_names, counts_list, strict=False))

    dag_steps: dict[str, Any] = {}
    for idx, name in enumerate(ordered_names):
        next_steps = [ordered_names[idx + 1]] if idx + 1 < len(ordered_names) else []
        prev_name = ordered_names[idx - 1] if idx > 0 else None
        count = task_counts.get(name, 1)
        prev_count = task_counts.get(prev_name, 1) if prev_name else 1
        if name == "start":
            step_type = "start"
        elif name == "end":
            step_type = "end"
        elif count > 1:
            step_type = "foreach"
        elif prev_count > 1 and count == 1:
            step_type = "join"
        else:
            step_type = "linear"
        dag_steps[name] = {
            "name": name,
            "type": step_type,
            "next": next_steps,
            "doc": "",
            "decorators": [],
        }

    dag = {
        "file": "",
        "parameters": [],
        "constants": [],
        "steps": dag_steps,
        "graph_structure": ordered_names,
        "doc": "",
        "decorators": [],
        "extensions": {},
    }
    return web.json_response({"data": dag, "links": {"next": None}})


async def _task_logs_handler(request: web.Request) -> web.Response:
    """
    Compatibility endpoint for task logs.

    Tries the upstream metadata endpoint first. If unavailable, falls back to
    GHA queue logs stored in S3 (combined stdout/stderr stream).
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")
    step_name = request.match_info.get("step_name", "")
    task_id = request.match_info.get("task_id", "")
    stream = request.match_info.get("stream", "out")
    resolved_task_id = await _resolve_task_identifier(
        session=session,
        service_url=service_url,
        headers=headers,
        flow_id=flow_id,
        run_id=str(run_id),
        step_name=step_name,
        task_identifier=str(task_id),
    )

    upstream = f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{resolved_task_id}/logs/{stream}"
    try:
        async with session.get(
            upstream,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            body = await resp.read()
            if resp.status == 200:
                return web.Response(
                    status=200,
                    body=_wrap_data_payload(body),
                    headers={"Content-Type": "application/json"},
                )
    except Exception:
        pass

    # Try reading from S3 datastore via task metadata.
    datastore_config: dict[str, Any] = request.app.get("datastore_config") or {}
    s3_lines = await _read_s3_task_log(
        session=session,
        service_url=service_url,
        headers=headers,
        flow_id=flow_id,
        run_id=str(run_id),
        step_name=step_name,
        task_id=str(resolved_task_id),
        stream=stream,
        datastore_config=datastore_config,
    )
    if s3_lines is not None:
        ts = int(time.time() * 1000)
        data = [{"row": i, "timestamp": ts, "line": line} for i, line in enumerate(s3_lines)]
        return web.json_response({"data": data, "links": {"next": None}})

    lines = _read_gha_task_log_lines(run_id=str(run_id), task_id=str(resolved_task_id))
    if not lines:
        return web.json_response({"data": [], "links": {"next": None}})

    ts = int(time.time() * 1000)
    data = [{"row": i, "timestamp": ts, "line": line} for i, line in enumerate(lines)]
    return web.json_response({"data": data})


def _normalize_runs_payload(payload: bytes) -> bytes:
    """
    Ensure run responses include ``run_id`` for UI compatibility.

    Some backends return ``run_id: null`` while the UI uses run_id to fetch
    run-specific resources. We backfill from run_number when needed.
    """
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception:
        return payload

    def _fix_run_obj(obj: dict[str, Any]) -> None:
        run_id = obj.get("run_id")
        run_number = obj.get("run_number")
        if run_id is None and run_number is not None:
            obj["run_id"] = str(run_number)
        if "user" not in obj and obj.get("user_name") is not None:
            obj["user"] = obj.get("user_name")
        if "status" not in obj:
            obj["status"] = _infer_run_status(obj)

    if isinstance(data, dict):
        _fix_run_obj(data)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _fix_run_obj(item)
    else:
        return payload

    try:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")
    except Exception:
        return payload


def _wrap_data_payload(payload: bytes) -> bytes:
    """
    Wrap JSON payload as {"data": ..., "links": {"next": null}} if not already wrapped.

    The Metaflow UI's fetch hook accesses ``response.links.next`` for pagination.
    Without a ``links`` field the property access throws a TypeError which is caught
    as a generic 500 error, so we always include ``links: {next: null}``.
    """
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception:
        return payload
    if isinstance(data, dict) and "data" in data:
        # Already wrapped — add links if absent.
        if "links" not in data:
            data["links"] = {"next": None}
            try:
                return json.dumps(data, separators=(",", ":")).encode("utf-8")
            except Exception:
                pass
        return payload
    try:
        return json.dumps({"data": data, "links": {"next": None}}, separators=(",", ":")).encode(
            "utf-8"
        )
    except Exception:
        return payload


def _infer_run_status(run: dict[str, Any]) -> str:
    """
    Best-effort run status for UI compatibility.
    """
    now_ms = int(time.time() * 1000)
    last_hb = run.get("last_heartbeat_ts")
    try:
        hb = int(last_hb) if last_hb is not None else None
    except Exception:
        hb = None
    if hb is not None and (now_ms - hb) < 5 * 60 * 1000:
        return "running"
    return "completed"


def _safe_ts(value: Any) -> int:
    """
    Best-effort timestamp normalization for sorting heterogeneous payloads.
    """
    try:
        if value is None:
            return 0
        return int(float(value))
    except Exception:
        return 0


async def _read_s3_task_log(
    session: aiohttp.ClientSession,
    service_url: str,
    headers: dict[str, str] | None,
    flow_id: str,
    run_id: str,
    step_name: str,
    task_id: str,
    stream: str,
    datastore_config: dict[str, Any],
) -> list[str] | None:
    """
    Read task log lines from the S3 datastore.

    Fetches task metadata to find the ds-root and log SHA.
    Metaflow stores log content in the content-addressed S3 store:
      {ds-root}/data/{sha[:2]}/{sha[2:]}

    The metadata fields ``log-stdout`` and ``log-stderr`` hold the SHA (or
    full path) for the respective stream.  If those fields are absent (e.g.
    for locally-executed tasks), returns None so callers can fall through.
    """
    # Map stream name to metadata field name.
    field_name = "log-stdout" if stream in ("stdout", "out") else "log-stderr"

    # Fetch task metadata.
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return None
            meta_data: Any = await resp.json()
    except Exception:
        return None

    entries = meta_data.get("data", meta_data) if isinstance(meta_data, dict) else meta_data
    if not isinstance(entries, list):
        return None

    ds_root: str | None = None
    log_sha: str | None = None
    for item in entries:
        if not isinstance(item, dict):
            continue
        if item.get("field_name") == "ds-root":
            ds_root = str(item.get("value", "")).strip()
        if item.get("field_name") == field_name:
            log_sha = str(item.get("value", "")).strip()

    # No log metadata → local run or log not persisted; return None to fall through.
    if not log_sha or not ds_root:
        return None

    # ds-root is like "s3://bucket/prefix".  Strip the s3:// scheme.
    # Log object path: {ds-root}/data/{sha[:2]}/{sha[2:]}
    s3_path = ds_root.removeprefix("s3://")
    parts = s3_path.split("/", 1)
    if len(parts) != 2:
        return None
    bucket, prefix = parts[0], parts[1].rstrip("/")
    obj_key = f"{prefix}/data/{log_sha[:2]}/{log_sha[2:]}"

    endpoint_url = datastore_config.get("METAFLOW_S3_ENDPOINT_URL")
    access_key = datastore_config.get("AWS_ACCESS_KEY_ID")
    secret_key = datastore_config.get("AWS_SECRET_ACCESS_KEY")
    region = datastore_config.get("AWS_DEFAULT_REGION", "us-east-1")

    # Try boto3 first (works for standard S3-compatible endpoints).
    try:
        import asyncio

        import boto3  # type: ignore

        def _boto_get() -> bytes:
            kwargs: dict[str, Any] = {"region_name": region}
            if endpoint_url:
                kwargs["endpoint_url"] = endpoint_url
            if access_key and secret_key:
                kwargs["aws_access_key_id"] = access_key
                kwargs["aws_secret_access_key"] = secret_key
            client = boto3.client("s3", **kwargs)
            resp = client.get_object(Bucket=bucket, Key=obj_key)
            return resp["Body"].read()

        raw = await asyncio.to_thread(_boto_get)
        try:
            import gzip

            raw = gzip.decompress(raw)
        except Exception:
            pass
        return raw.decode("utf-8", errors="replace").splitlines()
    except Exception:
        pass

    # Fallback: plain HTTPS GET with Bearer auth (works for Supabase Storage).
    if endpoint_url:
        try:
            # Supabase Storage download URL:
            # {endpoint_base}/object/authenticated/{bucket}/{key}
            endpoint_base = endpoint_url.rstrip("/").removesuffix("/s3")
            download_url = f"{endpoint_base}/object/authenticated/{bucket}/{obj_key}"
            dl_headers: dict[str, str] = {}
            if secret_key:
                dl_headers["Authorization"] = f"Bearer {secret_key}"
            async with session.get(
                download_url,
                headers=dl_headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    raw = await resp.read()
                    try:
                        import gzip

                        raw = gzip.decompress(raw)
                    except Exception:
                        pass
                    return raw.decode("utf-8", errors="replace").splitlines()
        except Exception:
            pass

    return None


def _read_gha_task_log_lines(run_id: str, task_id: str) -> list[str]:
    """
    Read task logs from the GHA S3 queue, if coordinator deps/config are present.
    """
    log_reader = _get_gha_log_reader()
    if log_reader is None:
        return []
    s3, bucket, prefix, read_task_log = log_reader

    try:
        content = read_task_log(s3, bucket, prefix, run_id, task_id)
    except Exception:
        return []

    if not content:
        return []
    return str(content).splitlines()


async def _task_attempts_handler(request: web.Request) -> web.Response:
    """
    Synthesize attempt objects for a task from its metadata.

    The metadata service has no native /attempts endpoint; we build the response
    from task data + metadata entries tagged with ``attempt_id:<N>``.
    """
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")
    step_name = request.match_info.get("step_name", "")
    task_id = request.match_info.get("task_id", "")

    resolved_id = await _resolve_task_identifier(
        session=session,
        service_url=service_url,
        headers=headers,
        flow_id=flow_id,
        run_id=str(run_id),
        step_name=step_name,
        task_identifier=str(task_id),
    )

    task_url = f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{resolved_id}"
    try:
        async with session.get(
            task_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            raw_task: Any = await r.json() if r.status == 200 else {}
        async with session.get(
            f"{task_url}/metadata", headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            raw_meta: Any = await r.json() if r.status == 200 else {}
    except Exception:
        return web.json_response({"data": [], "links": {"next": None}})

    task = raw_task.get("data", raw_task) if isinstance(raw_task, dict) else {}
    if isinstance(task, list):
        task = task[0] if task else {}

    if isinstance(raw_meta, list):
        meta_entries = raw_meta
    elif isinstance(raw_meta, dict):
        meta_entries = raw_meta.get("data", [])
        if not isinstance(meta_entries, list):
            meta_entries = []
    else:
        meta_entries = []

    # Group metadata by attempt_id
    by_attempt: dict[int, dict[str, Any]] = {}
    for entry in meta_entries:
        aid = 0
        for tag in entry.get("tags") or []:
            if tag.startswith("attempt_id:"):
                with contextlib.suppress(IndexError, ValueError):
                    aid = int(tag.split(":", 1)[1])
        if aid not in by_attempt:
            by_attempt[aid] = {}
        by_attempt[aid][entry.get("field_name", "")] = entry

    if not by_attempt:
        by_attempt[0] = {}

    attempts = []
    started_at = task.get("ts_epoch")
    for aid, fields in sorted(by_attempt.items()):
        done_entry = fields.get("attempt-done") or fields.get("attempt_ok")
        finished_at = done_entry.get("ts_epoch") if done_entry else None
        ok_entry = fields.get("attempt_ok")
        ok_val = ok_entry.get("value") if ok_entry else None
        if ok_val == "True":
            status = "completed"
        elif ok_val == "False":
            status = "failed"
        else:
            status = "running"
        attempts.append(
            {
                "flow_id": flow_id,
                "run_number": task.get("run_number"),
                "run_id": str(run_id),
                "step_name": step_name,
                "task_id": task.get("task_id"),
                "task_name": task.get("task_name") or task_id,
                "attempt_id": aid,
                "ts_epoch": started_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration": (finished_at - started_at) if (finished_at and started_at) else None,
                "status": status,
                "user_name": task.get("user_name"),
                "tags": task.get("tags", []),
                "system_tags": task.get("system_tags", []),
            }
        )

    return web.json_response({"data": attempts, "links": {"next": None}})


async def _task_detail_handler(request: web.Request) -> web.Response:
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")
    step_name = request.match_info.get("step_name", "")
    task_id = request.match_info.get("task_id", "")
    resolved_task_id = await _resolve_task_identifier(
        session=session,
        service_url=service_url,
        headers=headers,
        flow_id=flow_id,
        run_id=str(run_id),
        step_name=step_name,
        task_identifier=str(task_id),
    )
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{resolved_task_id}",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            body = await resp.read()
            if resp.status == 200:
                return web.Response(
                    status=200,
                    body=_wrap_data_payload(body),
                    headers={"Content-Type": "application/json"},
                )
            return web.json_response({"data": [], "links": {"next": None}})
    except Exception:
        return web.json_response({"data": [], "links": {"next": None}})


async def _task_metadata_handler(request: web.Request) -> web.Response:
    session: aiohttp.ClientSession = request.app["session"]
    service_url: str = request.app["service_url"]
    service_auth_key: str | None = request.app.get("service_auth_key")
    headers = {"x-api-key": service_auth_key} if service_auth_key else None
    flow_id = request.match_info.get("flow_id", "")
    run_id = request.match_info.get("run_id", "")
    step_name = request.match_info.get("step_name", "")
    task_id = request.match_info.get("task_id", "")
    resolved_task_id = await _resolve_task_identifier(
        session=session,
        service_url=service_url,
        headers=headers,
        flow_id=flow_id,
        run_id=str(run_id),
        step_name=step_name,
        task_identifier=str(task_id),
    )
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{resolved_task_id}/metadata",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            body = await resp.read()
            if resp.status == 200:
                return web.Response(
                    status=200,
                    body=_wrap_data_payload(body),
                    headers={"Content-Type": "application/json"},
                )
            return web.json_response({"data": [], "links": {"next": None}})
    except Exception:
        return web.json_response({"data": [], "links": {"next": None}})


async def _resolve_task_identifier(
    session: aiohttp.ClientSession,
    service_url: str,
    headers: dict[str, str] | None,
    flow_id: str,
    run_id: str,
    step_name: str,
    task_identifier: str,
) -> str:
    """
    Resolve either numeric task_id or task_name to a numeric task_id string.
    """
    if task_identifier.isdigit():
        return task_identifier
    try:
        async with session.get(
            f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return task_identifier
            data: Any = await resp.json()
        tasks = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(tasks, list):
            return task_identifier
        for task in tasks:
            if not isinstance(task, dict):
                continue
            if str(task.get("task_name", "")) == task_identifier:
                tid = task.get("task_id")
                if tid is not None:
                    return str(tid)
    except Exception:
        return task_identifier
    return task_identifier


@lru_cache(maxsize=1)
def _get_gha_log_reader() -> tuple[Any, str, str, Any] | None:
    """
    Build and cache the S3 log reader dependencies once per process.
    """
    try:
        import boto3  # type: ignore
        from metaflow_coordinator.s3_queue import _bucket_prefix_from_env, read_task_log
    except Exception:
        return None

    try:
        bucket, prefix = _bucket_prefix_from_env()
    except Exception:
        return None

    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("METAFLOW_S3_ENDPOINT_URL")
    kwargs: dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    try:
        s3 = boto3.client("s3", **kwargs)
    except Exception:
        return None

    return (s3, bucket, prefix, read_task_log)


async def _infer_task_status(
    session: aiohttp.ClientSession,
    service_url: str,
    headers: dict[str, str] | None,
    flow_id: str,
    run_id: str,
    step_name: str,
    task: dict[str, Any],
) -> str:
    """
    Best-effort task status for UI compatibility.
    """
    step = str(task.get("step_name") or step_name)
    if step == "_parameters":
        return "completed"

    task_ok = task.get("task_ok")
    if isinstance(task_ok, bool):
        return "completed" if task_ok else "failed"
    if isinstance(task_ok, str):
        v = task_ok.strip().lower()
        if v in {"true", "1", "ok", ":root:s3"}:
            return "completed"
        if v in {"false", "0", "error", "failed"}:
            return "failed"

    task_id = task.get("task_id")
    if task_id is not None:
        try:
            async with session.get(
                f"{service_url}/flows/{flow_id}/runs/{run_id}/steps/{step}/tasks/{task_id}/metadata",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    meta_data: Any = await resp.json()
                    entries = (
                        meta_data.get("data", meta_data)
                        if isinstance(meta_data, dict)
                        else meta_data
                    )
                    if isinstance(entries, list):
                        attempt_ok = None
                        for item in entries:
                            if not isinstance(item, dict):
                                continue
                            if item.get("field_name") == "attempt_ok":
                                attempt_ok = str(item.get("value", "")).strip().lower()
                        if attempt_ok in {"true", "1"}:
                            return "completed"
                        if attempt_ok in {"false", "0"}:
                            return "failed"
        except Exception:
            pass

    # If heartbeat is recent and completion markers are missing, treat as running.
    now_ms = int(time.time() * 1000)
    last_hb = task.get("last_heartbeat_ts") or task.get("ts_epoch")
    try:
        hb = int(last_hb) if last_hb is not None else None
    except Exception:
        hb = None
    if hb is not None and (now_ms - hb) < 5 * 60 * 1000:
        return "running"

    return "completed"


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
    service_auth_key: str | None = request.app.get("service_auth_key")

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    last_state: dict[str, Any] = {}

    async with aiohttp.ClientSession() as session:
        while not ws.closed:
            try:
                if service_auth_key:
                    current = await _fetch_active_state(
                        session,
                        service_url,
                        service_auth_key=service_auth_key,
                    )
                else:
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
    service_auth_key: str | None = None,
) -> dict[str, Any]:
    """
    Fetch runs that have had a heartbeat in the last 300 seconds.

    Queries the metadata service's ``/flows`` and ``/runs`` endpoints to
    build a mapping of ``run_id -> run_info`` for currently active runs.

    Returns an empty dict if the service is unreachable or returns an error.
    """
    try:
        headers = {"x-api-key": service_auth_key} if service_auth_key else None
        # Fetch all flows first.
        async with session.get(
            f"{service_url}/flows",
            headers=headers,
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
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as runs_resp:
                    if runs_resp.status != 200:
                        continue
                    runs_data: dict[str, Any] = await runs_resp.json()

                runs = (
                    runs_data.get("data", runs_data) if isinstance(runs_data, dict) else runs_data
                )
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

    async with (
        aiohttp.ClientSession() as session,
        session.get(
            download_url,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp,
    ):
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
            raise RuntimeError(f"Unexpected Metaflow UI archive format: {asset_name!r}")

        # Find the index.html in the extracted files.
        index_candidates = list(tmp_path.rglob("index.html"))
        if not index_candidates:
            raise RuntimeError("Could not find index.html in the downloaded Metaflow UI archive.")

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

    async with (
        aiohttp.ClientSession() as session,
        session.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp,
    ):
        if resp.status == 404:
            raise RuntimeError(f"Metaflow UI GitHub repository {_UI_REPO!r} not found.")
        resp.raise_for_status()
        release: dict[str, Any] = await resp.json()

    tag: str = release.get("tag_name", "unknown")
    assets: list[dict[str, Any]] = release.get("assets", [])

    # Look for a .tar.gz or .zip build asset (not source code).
    build_asset = None
    for asset in assets:
        name: str = asset["name"].lower()
        if ("build" in name or "dist" in name or "ui" in name) and (
            name.endswith(".tar.gz") or name.endswith(".zip") or name.endswith(".tgz")
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
