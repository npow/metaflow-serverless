"""
Tests for metaflow_ephemeral.installer logic.
"""
from __future__ import annotations

import shutil
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from metaflow_ephemeral.installer import _pick_asset, ensure_cli


class TestEnsureCli:
    async def test_ensure_cli_already_installed(self):
        """If shutil.which returns a path, ensure_cli returns True with no install attempt."""
        with patch("shutil.which", return_value="/usr/local/bin/neonctl"):
            # _INSTALLERS would never be called because we short-circuit at which()
            result = await ensure_cli("neonctl")
        assert result is True

    async def test_ensure_cli_unknown_no_installer(self):
        """Raises RuntimeError for CLI with no automatic installer."""
        with patch("shutil.which", return_value=None):
            with pytest.raises(RuntimeError, match="No automatic installer available"):
                await ensure_cli("totally-unknown-cli-xyz")


class TestPickAsset:
    """Tests for the _pick_asset function."""

    MOCK_ASSETS = [
        {"name": "mytool-darwin-arm64.tar.gz",  "browser_download_url": "https://example.com/darwin-arm64"},
        {"name": "mytool-darwin-amd64.tar.gz",  "browser_download_url": "https://example.com/darwin-amd64"},
        {"name": "mytool-linux-amd64.tar.gz",   "browser_download_url": "https://example.com/linux-amd64"},
        {"name": "mytool-linux-arm64.tar.gz",   "browser_download_url": "https://example.com/linux-arm64"},
        {"name": "mytool-darwin-arm64.tar.gz.sha256", "browser_download_url": "https://example.com/checksum"},
        {"name": "mytool-darwin-arm64.tar.gz.sig",    "browser_download_url": "https://example.com/sig"},
    ]

    def test_install_picks_correct_platform_asset_darwin_arm64(self):
        """_pick_asset selects darwin/arm64 asset correctly."""
        asset = _pick_asset(
            self.MOCK_ASSETS,
            os_variants=["darwin", "macos", "mac"],
            arch_variants=["arm64", "aarch64"],
        )
        assert asset is not None
        assert "darwin" in asset["name"].lower()
        assert "arm64" in asset["name"].lower()

    def test_install_picks_correct_platform_asset_linux_amd64(self):
        """_pick_asset selects linux/amd64 asset correctly."""
        asset = _pick_asset(
            self.MOCK_ASSETS,
            os_variants=["linux"],
            arch_variants=["amd64", "x86_64", "x64"],
        )
        assert asset is not None
        assert "linux" in asset["name"].lower()
        assert "amd64" in asset["name"].lower()

    def test_install_skips_checksum_files(self):
        """_pick_asset never returns .sha256 checksum files."""
        asset = _pick_asset(
            self.MOCK_ASSETS,
            os_variants=["darwin"],
            arch_variants=["arm64"],
        )
        assert asset is not None
        assert not asset["name"].endswith(".sha256")
        assert not asset["name"].endswith(".sig")

    def test_pick_asset_returns_none_when_no_match(self):
        """_pick_asset returns None if no suitable asset is found."""
        # Use an os_variant that is truly not a substring of any asset name.
        # ("win" is a substring of "darwin", so we use a clearly absent token.)
        assets_no_windows = [
            {"name": "mytool-linux-amd64.tar.gz",  "browser_download_url": "https://example.com/linux-amd64"},
            {"name": "mytool-linux-arm64.tar.gz",   "browser_download_url": "https://example.com/linux-arm64"},
        ]
        asset = _pick_asset(
            assets_no_windows,
            os_variants=["windows", "win64"],
            arch_variants=["amd64"],
        )
        assert asset is None

    def test_pick_asset_os_only_fallback(self):
        """_pick_asset falls back to OS-only match when arch doesn't match."""
        assets = [
            {"name": "mytool-darwin.tar.gz", "browser_download_url": "https://example.com/darwin"},
            {"name": "mytool-linux.tar.gz",  "browser_download_url": "https://example.com/linux"},
        ]
        asset = _pick_asset(
            assets,
            os_variants=["darwin"],
            arch_variants=["arm64"],
        )
        assert asset is not None
        assert "darwin" in asset["name"].lower()

    def test_pick_asset_skips_sig_files(self):
        """_pick_asset never returns .sig files."""
        assets_with_only_sig = [
            {"name": "mytool-darwin-arm64.tar.gz.sig", "browser_download_url": "https://example.com/sig"},
        ]
        asset = _pick_asset(
            assets_with_only_sig,
            os_variants=["darwin"],
            arch_variants=["arm64"],
        )
        assert asset is None

    def test_pick_asset_empty_list(self):
        """_pick_asset returns None for an empty asset list."""
        assert _pick_asset([], os_variants=["darwin"], arch_variants=["arm64"]) is None
