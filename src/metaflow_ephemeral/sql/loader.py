"""Helpers to load bundled SQL files as strings."""

import importlib.resources


def _read(filename: str) -> str:
    package = importlib.resources.files("metaflow_ephemeral.sql")
    return (package / filename).read_text(encoding="utf-8")


def load_schema() -> str:
    """Return the contents of schema.sql as a string."""
    return _read("schema.sql")


def load_procedures() -> str:
    """Return the contents of procedures.sql as a string."""
    return _read("procedures.sql")
