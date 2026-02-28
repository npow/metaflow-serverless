"""
Tests for the UI proxy module (metaflow_ephemeral.ui_proxy.proxy).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from metaflow_ephemeral.ui_proxy.proxy import (
    _build_app,
    _compute_diff,
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
        from metaflow_ephemeral.config import MetaflowConfig
        from metaflow_ephemeral.ui_proxy.proxy import run_proxy

        # Write a config with no service URL.
        cfg_path = tmp_path / ".metaflowconfig"
        cfg = MetaflowConfig(path=cfg_path)
        # Don't write any URL.

        with patch("metaflow_ephemeral.ui_proxy.proxy.MetaflowConfig") as mock_cfg_cls:
            mock_cfg = MagicMock()
            mock_cfg.get_service_url.return_value = None
            mock_cfg_cls.return_value = mock_cfg

            with pytest.raises(SystemExit) as exc_info:
                await run_proxy(port=19999)
            assert exc_info.value.code == 1
