"""
Tests for the bundled SQL files (schema.sql and procedures.sql).
"""

from __future__ import annotations

import pytest

from metaflow_serverless.sql.loader import load_procedures, load_schema


class TestSchemaLoads:
    def test_schema_loads(self):
        """load_schema() returns a non-empty string."""
        schema = load_schema()
        assert isinstance(schema, str)
        assert len(schema.strip()) > 0

    def test_schema_contains_tables(self):
        """schema.sql defines all 6 expected table names."""
        schema = load_schema()
        expected_tables = [
            "flows_v3",
            "runs_v3",
            "steps_v3",
            "tasks_v3",
            "metadata_v3",
            "artifact_v3",
        ]
        for table in expected_tables:
            assert table in schema, f"Table {table!r} not found in schema.sql"

    def test_schema_contains_create_table(self):
        """schema.sql uses CREATE TABLE statements."""
        schema = load_schema()
        assert "CREATE TABLE" in schema.upper()

    def test_schema_contains_primary_keys(self):
        """schema.sql defines primary key constraints."""
        schema = load_schema()
        assert "PRIMARY KEY" in schema.upper()

    def test_schema_contains_indexes(self):
        """schema.sql defines index creation statements."""
        schema = load_schema().upper()
        assert "CREATE INDEX" in schema or "CREATE UNIQUE INDEX" in schema


class TestProceduresLoads:
    def test_procedures_loads(self):
        """load_procedures() returns a non-empty string."""
        procs = load_procedures()
        assert isinstance(procs, str)
        assert len(procs.strip()) > 0

    def test_procedures_contains_functions(self):
        """procedures.sql defines all 9 expected function names."""
        procs = load_procedures()
        expected_functions = [
            "heartbeat_run",
            "heartbeat_task",
            "mutate_run_tags",
            "get_artifacts_latest",
            "filter_tasks_by_metadata",
            "create_flow",
            "create_run",
            "create_step",
            "create_task",
        ]
        for fn in expected_functions:
            assert fn in procs, f"Function {fn!r} not found in procedures.sql"

    def test_procedures_contains_create_function(self):
        """procedures.sql uses CREATE OR REPLACE FUNCTION statements."""
        procs = load_procedures()
        assert "CREATE OR REPLACE FUNCTION" in procs.upper()

    def test_procedures_contains_plpgsql(self):
        """procedures.sql uses PL/pgSQL language."""
        procs = load_procedures()
        assert "plpgsql" in procs.lower()


class TestSqlNoSyntaxErrors:
    @pytest.mark.skip(reason="Requires a live PostgreSQL connection")
    def test_schema_no_syntax_errors(self):
        """Schema SQL has no syntax errors (requires psycopg2 + live DB)."""
        # This test is intentionally skipped unless a Postgres connection is
        # available.  The skip marker documents the intent.
        pass
