"""
Tests for the UI proxy module (metaflow_serverless.ui_proxy.proxy).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp.test_utils import make_mocked_request

from metaflow_serverless.ui_proxy.proxy import (
    _build_app,
    _compute_diff,
    _proxy_handler,
    _task_detail_handler,
    _task_metadata_handler,
    _run_dag_handler,
    _run_tasks_handler,
    _runs_autocomplete_handler,
    _task_logs_handler,
)


# ---------------------------------------------------------------------------
# _compute_diff tests (pure unit tests — no I/O)
# ---------------------------------------------------------------------------

class TestComputeDiff:
    def test_compute_diff_empty(self):
        """_compute_diff({}, {}) returns {}."""
        assert _compute_diff({}, {}) == {}

    def test_compute_diff_new_key(self):
        """A new key in the second dict appears in the diff."""
        old = {"a": 1}
        new = {"a": 1, "b": 2}
        diff = _compute_diff(old, new)
        assert diff == {"b": 2}

    def test_compute_diff_changed_value(self):
        """A changed value in the second dict appears in the diff."""
        old = {"a": 1}
        new = {"a": 99}
        diff = _compute_diff(old, new)
        assert diff == {"a": 99}

    def test_compute_diff_unchanged(self):
        """Identical dicts return {}."""
        d = {"x": "hello", "y": [1, 2, 3]}
        assert _compute_diff(d, d) == {}

    def test_compute_diff_removed_key_not_in_diff(self):
        """Keys removed from new compared to old are NOT included in the diff."""
        old = {"a": 1, "b": 2}
        new = {"a": 1}
        diff = _compute_diff(old, new)
        assert diff == {}

    def test_compute_diff_all_new_keys(self):
        """Diff with an empty old dict returns all new items."""
        new = {"a": 1, "b": 2}
        diff = _compute_diff({}, new)
        assert diff == new

    def test_compute_diff_complex_values(self):
        """Diffs work correctly with complex nested values."""
        old = {"run1": {"status": "running", "ts": 100}}
        new = {"run1": {"status": "done", "ts": 200}}
        diff = _compute_diff(old, new)
        assert "run1" in diff
        assert diff["run1"]["status"] == "done"

    def test_compute_diff_no_changes_identical_complex(self):
        """No diff when complex values are identical."""
        state = {"run1": {"status": "running", "ts": 100}}
        assert _compute_diff(state, state) == {}


# ---------------------------------------------------------------------------
# _build_app tests
# ---------------------------------------------------------------------------

class TestBuildApp:
    def test_build_app_returns_application(self, tmp_path):
        """_build_app returns an aiohttp Application."""
        from aiohttp import web
        app = _build_app(service_url="https://example.com", ui_dir=tmp_path)
        assert isinstance(app, web.Application)

    def test_build_app_stores_service_url(self, tmp_path):
        """The service URL is stored in the app's state dict."""
        app = _build_app(service_url="https://myservice.run.app", ui_dir=tmp_path)
        assert app["service_url"] == "https://myservice.run.app"

    def test_build_app_stores_ui_dir(self, tmp_path):
        """The ui_dir is stored in the app's state dict."""
        app = _build_app(service_url="https://example.com", ui_dir=tmp_path)
        assert app["ui_dir"] == tmp_path

    def test_build_app_stores_service_auth_key(self, tmp_path):
        """The service auth key is stored in app state for upstream requests."""
        app = _build_app(
            service_url="https://example.com",
            ui_dir=tmp_path,
            service_auth_key="abc123",
        )
        assert app["service_auth_key"] == "abc123"

    def test_build_app_has_routes(self, tmp_path):
        """The app has routes defined (ws, api proxy, static)."""
        app = _build_app(service_url="https://example.com", ui_dir=tmp_path)
        # We can check that the router has resources registered.
        resources = list(app.router.resources())
        assert len(resources) > 0

    def test_build_app_has_lifecycle_hooks(self, tmp_path):
        """The app has startup and cleanup lifecycle hooks."""
        app = _build_app(service_url="https://example.com", ui_dir=tmp_path)
        assert len(app.on_startup) > 0
        assert len(app.on_cleanup) > 0


# ---------------------------------------------------------------------------
# run_proxy test (integration-level, mocked heavily)
# ---------------------------------------------------------------------------

class TestRunProxy:
    async def test_run_proxy_exits_if_no_service_url(self, tmp_path):
        """run_proxy calls sys.exit(1) if no service URL is configured."""
        import sys
        from metaflow_serverless.config import MetaflowConfig
        from metaflow_serverless.ui_proxy.proxy import run_proxy

        # Write a config with no service URL.
        cfg_path = tmp_path / ".metaflowconfig"
        cfg = MetaflowConfig(path=cfg_path)
        # Don't write any URL.

        with patch("metaflow_serverless.ui_proxy.proxy.MetaflowConfig") as mock_cfg_cls:
            mock_cfg = MagicMock()
            mock_cfg.get_service_url.return_value = None
            mock_cfg_cls.return_value = mock_cfg

            with pytest.raises(SystemExit) as exc_info:
                await run_proxy(port=19999)
            assert exc_info.value.code == 1


class _FakeResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload
        self.headers = {"content-type": "application/json"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def json(self):
        return self._payload

    async def read(self):
        import json as _json
        return _json.dumps(self._payload).encode("utf-8")


class _FakeSession:
    def __init__(self, responses):
        self._responses = responses

    def _pick(self, url: str):
        if url in self._responses:
            status, payload = self._responses[url]
            return _FakeResponse(status, payload)
        raise AssertionError(f"Unexpected URL requested: {url}")

    def get(self, url, **_kwargs):
        return self._pick(url)

    def request(self, method, url, **_kwargs):
        assert method == "GET"
        return self._pick(url)


class TestUiProxyCompatibility:
    @pytest.mark.asyncio
    async def test_proxy_wraps_and_normalizes_run_payload(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13": (
                200,
                {
                    "flow_id": "FlowA",
                    "run_number": 13,
                    "run_id": None,
                    "user_name": "alice",
                    "ts_epoch": 1,
                    "last_heartbeat_ts": 1,
                },
            ),
        })
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13",
            app=app,
            match_info={"path_tail": "flows/FlowA/runs/13"},
        )
        resp = await _proxy_handler(req)
        assert resp.status == 200
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        assert "data" in payload
        assert payload["data"]["run_id"] == "13"
        assert payload["data"]["user"] == "alice"
        assert payload["data"]["status"] in {"running", "completed"}

    @pytest.mark.asyncio
    async def test_runs_autocomplete_returns_objects(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs": (
                200,
                [
                    {"run_number": 13, "user_name": "alice", "ts_epoch": 100},
                    {"run_number": 12, "user_name": "alice", "ts_epoch": 90},
                ],
            ),
        })
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/autocomplete?_limit=5&run%3Aco=13",
            app=app,
            match_info={"flow_id": "FlowA"},
        )
        resp = await _runs_autocomplete_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        assert isinstance(payload, list)
        assert len(payload) == 1
        assert payload[0]["run_id"] == "13"
        assert payload[0]["user"] == "alice"
        assert payload[0]["status"] in {"running", "completed"}

    @pytest.mark.asyncio
    async def test_run_tasks_handler_enriches_status(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/steps": (
                200,
                [
                    {"step_name": "_parameters", "ts_epoch": 1},
                    {"step_name": "start", "ts_epoch": 2},
                    {"step_name": "fanout", "ts_epoch": 3},
                ],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/_parameters/tasks": (
                200,
                [{"task_id": 1, "step_name": "_parameters", "ts_epoch": 11, "user_name": "alice"}],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks": (
                200,
                [{"task_id": 2, "step_name": "start", "ts_epoch": 12, "user_name": "alice"}],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks": (
                200,
                [{"task_id": 3, "step_name": "fanout", "ts_epoch": 13, "user_name": "alice"}],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks/2/metadata": (
                200,
                [{"field_name": "attempt_ok", "value": "True"}],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks/3/metadata": (
                200,
                [{"field_name": "attempt_ok", "value": "False"}],
            ),
        })
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/tasks",
            app=app,
            match_info={"flow_id": "FlowA", "run_id": "13"},
        )
        resp = await _run_tasks_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        tasks = payload["data"]
        assert len(tasks) == 3
        statuses = {str(t["task_id"]): t["status"] for t in tasks}
        assert statuses["1"] == "completed"
        assert statuses["2"] == "completed"
        assert statuses["3"] == "failed"
        assert all(t["run_id"] == "13" for t in tasks)

    @pytest.mark.asyncio
    async def test_run_dag_handler_synthesizes_linear_from_steps(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/dag": (404, {"error": "not found"}),
            f"{service_url}/flows/FlowA/runs/13/steps": (
                200,
                [
                    {"step_name": "_parameters", "ts_epoch": 1},
                    {"step_name": "start", "ts_epoch": 2},
                    {"step_name": "process", "ts_epoch": 3},
                    {"step_name": "end", "ts_epoch": 4},
                ],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks": (200, [{"task_id": 1}]),
            f"{service_url}/flows/FlowA/runs/13/steps/process/tasks": (200, [{"task_id": 2}]),
            f"{service_url}/flows/FlowA/runs/13/steps/end/tasks": (200, [{"task_id": 3}]),
        })
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/dag",
            app=app,
            match_info={"flow_id": "FlowA", "run_id": "13"},
        )
        resp = await _run_dag_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        dag = payload["data"]
        assert dag["graph_structure"] == ["start", "process", "end"]
        assert dag["steps"]["start"]["type"] == "start"
        assert dag["steps"]["process"]["type"] == "linear"
        assert dag["steps"]["end"]["type"] == "end"
        assert dag["steps"]["start"]["next"] == ["process"]
        assert dag["steps"]["process"]["next"] == ["end"]
        assert dag["steps"]["end"]["next"] == []

    @pytest.mark.asyncio
    async def test_run_dag_handler_infers_foreach_join_types(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/dag": (404, {"error": "not found"}),
            f"{service_url}/flows/FlowA/runs/13/steps": (
                200,
                [
                    {"step_name": "start", "ts_epoch": 1},
                    {"step_name": "fanout", "ts_epoch": 2},
                    {"step_name": "join", "ts_epoch": 3},
                    {"step_name": "end", "ts_epoch": 4},
                ],
            ),
            # fanout has 3 parallel tasks → foreach
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks": (200, [{"task_id": 1}]),
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks": (
                200,
                [{"task_id": 2}, {"task_id": 3}, {"task_id": 4}],
            ),
            # join has 1 task following a foreach → join type
            f"{service_url}/flows/FlowA/runs/13/steps/join/tasks": (200, [{"task_id": 5}]),
            f"{service_url}/flows/FlowA/runs/13/steps/end/tasks": (200, [{"task_id": 6}]),
        })
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/dag",
            app=app,
            match_info={"flow_id": "FlowA", "run_id": "13"},
        )
        resp = await _run_dag_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        dag = payload["data"]
        assert dag["steps"]["start"]["type"] == "start"
        assert dag["steps"]["fanout"]["type"] == "foreach"
        assert dag["steps"]["join"]["type"] == "join"
        assert dag["steps"]["end"]["type"] == "end"

    @pytest.mark.asyncio
    async def test_task_logs_handler_falls_back_to_gha_logs(self, tmp_path, monkeypatch):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        # Upstream 404 + no metadata log entries → falls through to GHA reader
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks/2/logs/out": (
                404,
                {"error": "not found"},
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks/2/metadata": (
                200,
                [{"field_name": "attempt_ok", "value": "True"}],
            ),
        })
        monkeypatch.setattr(
            "metaflow_serverless.ui_proxy.proxy._read_gha_task_log_lines",
            lambda run_id, task_id: ["line a", "line b"],
        )
        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/steps/start/tasks/2/logs/out",
            app=app,
            match_info={
                "flow_id": "FlowA",
                "run_id": "13",
                "step_name": "start",
                "task_id": "2",
                "stream": "out",
            },
        )
        resp = await _task_logs_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        assert "data" in payload
        assert [r["line"] for r in payload["data"]] == ["line a", "line b"]

    @pytest.mark.asyncio
    async def test_task_logs_handler_reads_s3_when_log_metadata_present(self, tmp_path, monkeypatch):
        service_url = "https://service.example"
        app = _build_app(
            service_url=service_url,
            ui_dir=tmp_path,
            datastore_config={
                "METAFLOW_S3_ENDPOINT_URL": "https://s3.example",
                "AWS_ACCESS_KEY_ID": "key",
                "AWS_SECRET_ACCESS_KEY": "secret",
            },
        )
        sha = "aabbcc1234567890" * 2  # 32-char sha
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks/2/logs/stdout": (
                404,
                {"error": "not found"},
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/start/tasks/2/metadata": (
                200,
                [
                    {"field_name": "ds-root", "value": "s3://my-bucket/metaflow"},
                    {"field_name": "log-stdout", "value": sha},
                ],
            ),
        })

        # Patch boto3 to simulate a successful S3 read.
        import types
        fake_boto3 = types.ModuleType("boto3")
        fake_client = MagicMock()
        fake_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"hello\nworld\n")
        }
        fake_boto3.client = MagicMock(return_value=fake_client)
        monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

        req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/steps/start/tasks/2/logs/stdout",
            app=app,
            match_info={
                "flow_id": "FlowA",
                "run_id": "13",
                "step_name": "start",
                "task_id": "2",
                "stream": "stdout",
            },
        )
        resp = await _task_logs_handler(req)
        payload = __import__("json").loads(resp.body.decode("utf-8"))
        assert "data" in payload
        assert [r["line"] for r in payload["data"]] == ["hello", "world"]

    @pytest.mark.asyncio
    async def test_task_handlers_resolve_task_name_to_task_id(self, tmp_path):
        service_url = "https://service.example"
        app = _build_app(service_url=service_url, ui_dir=tmp_path)
        app["session"] = _FakeSession({
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks": (
                200,
                [{"task_id": 57, "task_name": "auto-abc"}],
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks/57": (
                200,
                {"task_id": 57, "task_name": "auto-abc"},
            ),
            f"{service_url}/flows/FlowA/runs/13/steps/fanout/tasks/57/metadata": (
                200,
                [{"field_name": "attempt_ok", "value": "True"}],
            ),
        })

        detail_req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/steps/fanout/tasks/auto-abc",
            app=app,
            match_info={
                "flow_id": "FlowA",
                "run_id": "13",
                "step_name": "fanout",
                "task_id": "auto-abc",
            },
        )
        detail_resp = await _task_detail_handler(detail_req)
        detail_payload = __import__("json").loads(detail_resp.body.decode("utf-8"))
        assert detail_payload["data"]["task_id"] == 57

        meta_req = make_mocked_request(
            "GET",
            "/api/flows/FlowA/runs/13/steps/fanout/tasks/auto-abc/metadata",
            app=app,
            match_info={
                "flow_id": "FlowA",
                "run_id": "13",
                "step_name": "fanout",
                "task_id": "auto-abc",
            },
        )
        meta_resp = await _task_metadata_handler(meta_req)
        meta_payload = __import__("json").loads(meta_resp.body.decode("utf-8"))
        assert meta_payload["data"][0]["field_name"] == "attempt_ok"
