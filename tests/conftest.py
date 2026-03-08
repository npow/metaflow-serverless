"""
Shared pytest fixtures for the metaflow-ephemeral-service test suite.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """
    Patch Path.home() to point at a temporary directory so that config tests
    do not touch the real ~/.metaflowconfig.

    Also patches os.path.expanduser so that code using '~' resolves correctly.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("HOME", str(fake_home))
    return fake_home


@pytest.fixture
def mock_subprocess():
    """
    Patch asyncio.create_subprocess_exec to return a mock process that exits
    successfully with empty stdout/stderr.

    Returns the mock process object so tests can configure it further.
    """
    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate = AsyncMock(return_value=(b"", b""))

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as patched:
        yield patched, mock_proc
