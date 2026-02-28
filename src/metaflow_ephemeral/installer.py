"""
CLI tool detection and installation helpers.

Provides ``ensure_cli(cli_name)`` which checks if a CLI is on the PATH and
installs it automatically when possible, with per-tool install logic.
"""

from __future__ import annotations

import asyncio
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import httpx

# Directory where binaries are installed when not using a package manager.
_LOCAL_BIN = Path.home() / ".local" / "bin"

# GitHub API base for release fetching.
_GITHUB_API = "https://api.github.com"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def ensure_cli(cli_name: str) -> bool:
    """
    Ensure that *cli_name* is available on the PATH.

    If the binary is already found via ``shutil.which``, returns ``True``
    immediately (no-op).  Otherwise, attempts to install it using the
    appropriate method for each known tool.

    Parameters
    ----------
    cli_name:
        Binary name to check/install.  Supported values: "supabase",
        "neonctl", "wrangler", "gcloud", "render".

    Returns
    -------
    bool
        ``True`` if the CLI is (or was) successfully installed.

    Raises
    ------
    RuntimeError
        If installation fails or the CLI is unknown and cannot be found.
    """
    if shutil.which(cli_name):
        return True

    print(f"[installer] {cli_name!r} not found on PATH; attempting installation...")

    installer = _INSTALLERS.get(cli_name)
    if installer is None:
        raise RuntimeError(
            f"No automatic installer available for {cli_name!r}. "
            f"Please install it manually and ensure it is on your PATH."
        )

    await installer()

    if shutil.which(cli_name):
        return True

    # Add _LOCAL_BIN to PATH for the current process in case the binary was
    # just installed there.
    _add_local_bin_to_path()
    if shutil.which(cli_name):
        return True

    raise RuntimeError(
        f"Installation of {cli_name!r} appeared to succeed but the binary "
        f"still cannot be found.  Try adding {_LOCAL_BIN} to your PATH:\n"
        f"  export PATH=\"$HOME/.local/bin:$PATH\""
    )


# ---------------------------------------------------------------------------
# Per-tool install functions
# ---------------------------------------------------------------------------


async def _install_supabase() -> None:
    """
    Install the Supabase CLI.

    Tries Homebrew first (macOS/Linux), then falls back to downloading the
    binary from the GitHub releases for the ``supabase/cli`` repository.
    """
    if _try_brew("supabase/tap/supabase"):
        return
    await _install_from_github_releases(
        repo="supabase/cli",
        binary_name="supabase",
    )


async def _install_neonctl() -> None:
    """
    Install neonctl (the Neon CLI).

    Tries npm first (cross-platform), then falls back to GitHub releases.
    """
    if await _try_npm("neonctl"):
        return
    await _install_from_github_releases(
        repo="neondatabase/neonctl",
        binary_name="neonctl",
    )


async def _install_wrangler() -> None:
    """
    Install Wrangler (the Cloudflare developer platform CLI) via npm.

    Wrangler is a Node.js package and does not ship standalone binaries;
    npm is the only supported install method.
    """
    if await _try_npm("wrangler"):
        return
    raise RuntimeError(
        "Failed to install wrangler via npm.\n"
        "Please install Node.js (https://nodejs.org/) and then run:\n"
        "  npm install -g wrangler"
    )


async def _install_gcloud() -> None:
    """
    Install the Google Cloud SDK (gcloud CLI).

    Downloads the interactive installer script from dl.google.com and runs it
    in quiet/non-interactive mode.  The installer adds gcloud to the user's
    PATH via shell profile modification.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Determine the correct archive URL.
    if system == "darwin":
        arch = "arm" if machine in ("arm64", "aarch64") else "x86_64"
        filename = f"google-cloud-cli-darwin-{arch}.tar.gz"
    elif system == "linux":
        arch = "arm" if machine in ("arm64", "aarch64") else "x86_64"
        filename = f"google-cloud-cli-linux-{arch}.tar.gz"
    else:
        raise RuntimeError(
            f"Automatic gcloud installation is not supported on {system!r}. "
            f"Download manually from https://cloud.google.com/sdk/docs/install"
        )

    url = f"https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/{filename}"
    install_dir = Path.home() / ".local" / "share" / "google-cloud-sdk"
    install_dir.mkdir(parents=True, exist_ok=True)

    print(f"[installer] Downloading Google Cloud SDK from {url} ...")
    await _download_and_extract_tar(url, install_dir)

    # Run the install script to update PATH in shell profiles.
    sdk_install = install_dir / "google-cloud-sdk" / "install.sh"
    if sdk_install.exists():
        result = await asyncio.create_subprocess_exec(
            str(sdk_install),
            "--quiet",
            "--path-update", "true",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await result.communicate()

    # Symlink the gcloud binary into _LOCAL_BIN.
    gcloud_bin = install_dir / "google-cloud-sdk" / "bin" / "gcloud"
    if gcloud_bin.exists():
        symlink = _LOCAL_BIN / "gcloud"
        _LOCAL_BIN.mkdir(parents=True, exist_ok=True)
        if symlink.exists() or symlink.is_symlink():
            symlink.unlink()
        symlink.symlink_to(gcloud_bin)
        print(f"[installer] gcloud installed to {symlink}")
    else:
        raise RuntimeError(
            f"gcloud installation script ran but binary not found at {gcloud_bin}. "
            f"Check {install_dir} for details."
        )


async def _install_render() -> None:
    """
    Install the Render CLI from GitHub releases.

    The Render CLI repository is ``render-oss/render-cli``.
    """
    await _install_from_github_releases(
        repo="render-oss/render-cli",
        binary_name="render",
    )


# ---------------------------------------------------------------------------
# GitHub releases helper
# ---------------------------------------------------------------------------


async def _install_from_github_releases(
    repo: str,
    binary_name: str,
) -> None:
    """
    Download and install a binary from the latest GitHub release.

    Fetches the release metadata from the GitHub API, selects the right
    asset for the current OS and CPU architecture, downloads it, extracts
    it (tar.gz or zip), and installs the binary to ``~/.local/bin/``.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/name`` format, e.g. "neondatabase/neonctl".
    binary_name:
        Name of the binary to extract and install (e.g. "neonctl").  Used
        both to find the binary inside the archive and as the installed
        file name.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Normalise architecture names.
    if machine in ("x86_64", "amd64"):
        arch_variants = ["amd64", "x86_64", "x64"]
    elif machine in ("arm64", "aarch64"):
        arch_variants = ["arm64", "aarch64"]
    else:
        arch_variants = [machine]

    # Normalise OS names.
    if system == "darwin":
        os_variants = ["darwin", "macos", "mac"]
    elif system == "linux":
        os_variants = ["linux"]
    elif system == "windows":
        os_variants = ["windows", "win"]
    else:
        os_variants = [system]

    url = f"{_GITHUB_API}/repos/{repo}/releases/latest"
    print(f"[installer] Fetching latest release info from {url} ...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
        )
        if response.status_code == 404:
            raise RuntimeError(f"GitHub repository {repo!r} not found.")
        response.raise_for_status()
        release = response.json()

    assets: list[dict[str, Any]] = release.get("assets", [])
    tag: str = release.get("tag_name", "unknown")
    print(f"[installer] Latest release: {tag}")

    asset = _pick_asset(assets, os_variants, arch_variants)
    if asset is None:
        names = [a["name"] for a in assets]
        raise RuntimeError(
            f"Could not find a suitable asset for {system}/{machine} in {repo} {tag}.\n"
            f"Available assets: {names}\n"
            f"Please install {binary_name!r} manually."
        )

    download_url: str = asset["browser_download_url"]
    asset_name: str = asset["name"]
    print(f"[installer] Downloading {asset_name} from {download_url} ...")

    _LOCAL_BIN.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        dl_response = await client.get(download_url)
        dl_response.raise_for_status()
        data = dl_response.content

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        archive_path = tmp_path / asset_name

        archive_path.write_bytes(data)

        # Extract the archive.
        if asset_name.endswith(".tar.gz") or asset_name.endswith(".tgz"):
            _extract_tar(archive_path, tmp_path, binary_name)
        elif asset_name.endswith(".zip"):
            _extract_zip(archive_path, tmp_path, binary_name)
        else:
            # Assume it's a bare binary (no archive).
            shutil.copy2(archive_path, _LOCAL_BIN / binary_name)
            (_LOCAL_BIN / binary_name).chmod(
                (_LOCAL_BIN / binary_name).stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
            )
            print(f"[installer] Installed {binary_name} to {_LOCAL_BIN / binary_name}")
            return

        # Find the extracted binary in tmp.
        binary_path = _find_binary(tmp_path, binary_name)
        if binary_path is None:
            raise RuntimeError(
                f"Could not find binary {binary_name!r} inside the downloaded archive "
                f"{asset_name!r}.  Contents: {list(tmp_path.rglob('*'))}"
            )

        dest = _LOCAL_BIN / binary_name
        shutil.copy2(binary_path, dest)
        dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        print(f"[installer] Installed {binary_name} to {dest}")


def _pick_asset(
    assets: list[dict[str, Any]],
    os_variants: list[str],
    arch_variants: list[str],
) -> dict[str, Any] | None:
    """
    Select the best release asset for the current platform.

    Prefers assets whose filename contains both an OS token and an arch token.
    Falls back to OS-only matches if no arch match is found.
    """
    asset_lower = [(a, a["name"].lower()) for a in assets]

    # Try OS + arch match first.
    for os_token in os_variants:
        for arch_token in arch_variants:
            for asset, name in asset_lower:
                if os_token in name and arch_token in name:
                    # Skip .sha256, .sig, etc.
                    if any(name.endswith(ext) for ext in (".sha256", ".sig", ".asc", ".md5")):
                        continue
                    return asset

    # Fall back to OS-only match.
    for os_token in os_variants:
        for asset, name in asset_lower:
            if os_token in name:
                if any(name.endswith(ext) for ext in (".sha256", ".sig", ".asc", ".md5")):
                    continue
                return asset

    return None


def _extract_tar(archive: Path, dest: Path, binary_name: str) -> None:
    """Extract a .tar.gz archive to *dest*."""
    with tarfile.open(archive, "r:gz") as tf:
        tf.extractall(dest)


def _extract_zip(archive: Path, dest: Path, binary_name: str) -> None:
    """Extract a .zip archive to *dest*."""
    with zipfile.ZipFile(archive, "r") as zf:
        zf.extractall(dest)


def _find_binary(root: Path, binary_name: str) -> Path | None:
    """
    Recursively search *root* for a file named *binary_name* (or binary_name.exe).

    Returns the first match found (depth-first), or ``None`` if not present.
    """
    candidates = [binary_name, binary_name + ".exe"]
    for candidate in candidates:
        for path in root.rglob(candidate):
            if path.is_file():
                return path
    return None


async def _download_and_extract_tar(url: str, dest: Path) -> None:
    """Download a tar.gz from *url* and extract it to *dest*."""
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.content

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)

    try:
        with tarfile.open(tmp_path, "r:gz") as tf:
            tf.extractall(dest)
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Package manager helpers
# ---------------------------------------------------------------------------


def _try_brew(package: str) -> bool:
    """
    Attempt to install *package* via Homebrew.

    Returns ``True`` if installation succeeded, ``False`` if brew is not
    available or installation failed.
    """
    if not shutil.which("brew"):
        return False
    print(f"[installer] Installing via Homebrew: brew install {package}")
    result = subprocess.run(
        ["brew", "install", package],
        text=True,
        capture_output=False,
    )
    return result.returncode == 0


async def _try_npm(package: str) -> bool:
    """
    Attempt to install *package* globally via npm.

    Returns ``True`` if npm is available and installation succeeded.
    """
    if not shutil.which("npm"):
        print(
            f"[installer] npm not found; cannot install {package!r} via npm.\n"
            f"Install Node.js from https://nodejs.org/ to enable npm installs."
        )
        return False

    print(f"[installer] Installing via npm: npm install -g {package}")
    proc = await asyncio.create_subprocess_exec(
        "npm", "install", "-g", package,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        print(f"[installer] npm install failed:\n{stderr.decode().strip()}")
        return False
    return True


# ---------------------------------------------------------------------------
# PATH helper
# ---------------------------------------------------------------------------


def _add_local_bin_to_path() -> None:
    """Add ~/.local/bin to the current process's PATH if not already present."""
    local_bin = str(_LOCAL_BIN)
    current_path = os.environ.get("PATH", "")
    if local_bin not in current_path.split(os.pathsep):
        os.environ["PATH"] = local_bin + os.pathsep + current_path


# ---------------------------------------------------------------------------
# Installer dispatch table
# ---------------------------------------------------------------------------

_INSTALLERS: dict[str, Any] = {
    "supabase": _install_supabase,
    "neonctl": _install_neonctl,
    "wrangler": _install_wrangler,
    "gcloud": _install_gcloud,
    "render": _install_render,
}
