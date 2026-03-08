"""
Tests for metaflow_serverless.config (MetaflowConfig and StackConfig).
"""

from __future__ import annotations

import json

import pytest

from metaflow_serverless.config import MetaflowConfig, StackConfig


class TestMetaflowConfig:
    """Tests for MetaflowConfig read/write behaviour."""

    def test_read_nonexistent(self, tmp_path):
        """Returns an empty dict when the config file does not exist."""
        cfg = MetaflowConfig(path=tmp_path / "does_not_exist.json")
        assert cfg.read() == {}

    def test_write_creates_file(self, tmp_path):
        """write() creates the config file if it doesn't exist."""
        path = tmp_path / "subdir" / ".metaflowconfig"
        cfg = MetaflowConfig(path=path)
        cfg.write({"FOO": "bar"})
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["FOO"] == "bar"

    def test_write_merges(self, tmp_path):
        """Existing keys are preserved; new keys are added."""
        path = tmp_path / ".metaflowconfig"
        path.write_text(json.dumps({"EXISTING_KEY": "keep_me"}))
        cfg = MetaflowConfig(path=path)
        cfg.write({"NEW_KEY": "hello"})
        data = json.loads(path.read_text())
        assert data["EXISTING_KEY"] == "keep_me"
        assert data["NEW_KEY"] == "hello"

    def test_write_overwrites_existing_key(self, tmp_path):
        """Values in the new data take precedence over existing values."""
        path = tmp_path / ".metaflowconfig"
        path.write_text(json.dumps({"KEY": "old"}))
        cfg = MetaflowConfig(path=path)
        cfg.write({"KEY": "new"})
        data = json.loads(path.read_text())
        assert data["KEY"] == "new"

    def test_get_service_url(self, tmp_path):
        """get_service_url() returns the METAFLOW_SERVICE_URL value."""
        path = tmp_path / ".metaflowconfig"
        path.write_text(json.dumps({"METAFLOW_SERVICE_URL": "https://example.com"}))
        cfg = MetaflowConfig(path=path)
        assert cfg.get_service_url() == "https://example.com"

    def test_get_service_url_missing(self, tmp_path):
        """get_service_url() returns None when the key is absent."""
        path = tmp_path / ".metaflowconfig"
        cfg = MetaflowConfig(path=path)
        assert cfg.get_service_url() is None

    def test_get_service_url_empty_string(self, tmp_path):
        """get_service_url() returns None when the value is an empty string."""
        path = tmp_path / ".metaflowconfig"
        path.write_text(json.dumps({"METAFLOW_SERVICE_URL": ""}))
        cfg = MetaflowConfig(path=path)
        assert cfg.get_service_url() is None

    def test_get_service_auth_key(self, tmp_path):
        """get_service_auth_key() returns the METAFLOW_SERVICE_AUTH_KEY value."""
        path = tmp_path / ".metaflowconfig"
        path.write_text(json.dumps({"METAFLOW_SERVICE_AUTH_KEY": "abc123"}))
        cfg = MetaflowConfig(path=path)
        assert cfg.get_service_auth_key() == "abc123"

    def test_get_service_auth_key_missing(self, tmp_path):
        """get_service_auth_key() returns None when key is absent."""
        cfg = MetaflowConfig(path=tmp_path / ".metaflowconfig")
        assert cfg.get_service_auth_key() is None

    def test_read_invalid_json(self, tmp_path):
        """Returns empty dict if the config file contains invalid JSON."""
        path = tmp_path / ".metaflowconfig"
        path.write_text("not valid json {{{{")
        cfg = MetaflowConfig(path=path)
        assert cfg.read() == {}

    def test_read_empty_file(self, tmp_path):
        """Returns empty dict if the config file is empty."""
        path = tmp_path / ".metaflowconfig"
        path.write_text("")
        cfg = MetaflowConfig(path=path)
        assert cfg.read() == {}

    def test_default_path_uses_config_json(self, tmp_home):
        """Default path points to ~/.metaflowconfig/config.json."""
        cfg = MetaflowConfig()
        assert str(cfg.path).endswith("/.metaflowconfig/config.json")

    def test_directory_path_resolves_to_config_json(self, tmp_path):
        """Passing a directory path writes config.json inside it."""
        cfg_dir = tmp_path / ".metaflowconfig"
        cfg_dir.mkdir()
        cfg = MetaflowConfig(path=cfg_dir)
        cfg.write({"FOO": "bar"})
        cfg_file = cfg_dir / "config.json"
        assert cfg_file.exists()
        data = json.loads(cfg_file.read_text())
        assert data["FOO"] == "bar"


class TestStackConfig:
    """Tests for StackConfig.validate()."""

    def test_valid_supabase_stack(self):
        """supabase/supabase/supabase is a valid stack."""
        s = StackConfig(compute="supabase", database="supabase", storage="supabase")
        s.validate()  # Should not raise.

    def test_valid_cloud_run_stack(self):
        """cloud-run/neon/r2 is a valid stack."""
        s = StackConfig(compute="cloud-run", database="neon", storage="r2")
        s.validate()  # Should not raise.

    def test_stackconfig_invalid_compute(self):
        """Raises ValueError for unknown compute provider."""
        s = StackConfig(compute="unknown-compute", database="neon", storage="r2")
        with pytest.raises(ValueError, match="Unknown compute provider"):
            s.validate()

    def test_stackconfig_invalid_database(self):
        """Raises ValueError when database is incompatible with compute."""
        # cloud-run is not compatible with supabase database
        s = StackConfig(compute="cloud-run", database="supabase", storage="r2")
        with pytest.raises(ValueError, match="not compatible with"):
            s.validate()

    def test_stackconfig_invalid_storage(self):
        """Raises ValueError when storage is incompatible with compute."""
        # cloud-run is not compatible with supabase storage
        s = StackConfig(compute="cloud-run", database="neon", storage="supabase")
        with pytest.raises(ValueError, match="not compatible with"):
            s.validate()
