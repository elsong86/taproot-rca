"""Tests for schema diffing, prompt hydration, and demo mode."""

import pytest

from taproot_rca.connectors.postgres import ColumnInfo, SchemaSnapshot, TableInfo
from taproot_rca.schema_diff import ChangeType, diff_snapshots
from taproot_rca.demo import get_demo_before, get_demo_after
from taproot_rca.prompt_engine import PromptContext, PromptEngine, PromptRole
from taproot_rca.config import TaprootConfig


# ---------------------------------------------------------------------------
# Schema diff tests
# ---------------------------------------------------------------------------

def _make_snapshot(tables, name="test"):
    return SchemaSnapshot(source_name=name, tables=tables, captured_at="2026-01-01T00:00:00Z")


class TestSchemaDiff:
    def test_identical_schemas_no_drift(self):
        t = TableInfo("public", "users", [ColumnInfo("id", "integer")])
        diff = diff_snapshots(_make_snapshot([t]), _make_snapshot([t]))
        assert not diff.has_drift
        assert len(diff.changes) == 0

    def test_table_added(self):
        before = _make_snapshot([])
        after = _make_snapshot([TableInfo("public", "users", [ColumnInfo("id", "integer")])])
        diff = diff_snapshots(before, after)
        assert diff.has_drift
        assert diff.changes[0].change_type == ChangeType.TABLE_ADDED

    def test_table_removed(self):
        before = _make_snapshot([TableInfo("public", "users", [ColumnInfo("id", "integer")])])
        after = _make_snapshot([])
        diff = diff_snapshots(before, after)
        assert diff.changes[0].change_type == ChangeType.TABLE_REMOVED

    def test_column_added(self):
        before = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("id", "integer")])
        ])
        after = _make_snapshot([
            TableInfo("public", "users", [
                ColumnInfo("id", "integer"),
                ColumnInfo("email", "varchar"),
            ])
        ])
        diff = diff_snapshots(before, after)
        assert any(c.change_type == ChangeType.COLUMN_ADDED for c in diff.changes)

    def test_column_removed(self):
        before = _make_snapshot([
            TableInfo("public", "users", [
                ColumnInfo("id", "integer"),
                ColumnInfo("email", "varchar"),
            ])
        ])
        after = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("id", "integer")])
        ])
        diff = diff_snapshots(before, after)
        assert any(c.change_type == ChangeType.COLUMN_REMOVED for c in diff.changes)

    def test_column_type_changed(self):
        before = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("amount", "numeric(10,2)")])
        ])
        after = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("amount", "numeric(15,2)")])
        ])
        diff = diff_snapshots(before, after)
        change = diff.changes[0]
        assert change.change_type == ChangeType.COLUMN_TYPE_CHANGED
        assert change.old_value == "numeric(10,2)"
        assert change.new_value == "numeric(15,2)"

    def test_nullable_changed(self):
        before = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("name", "varchar", is_nullable=False)])
        ])
        after = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("name", "varchar", is_nullable=True)])
        ])
        diff = diff_snapshots(before, after)
        assert diff.changes[0].change_type == ChangeType.COLUMN_NULLABLE_CHANGED

    def test_default_changed(self):
        before = _make_snapshot([
            TableInfo("public", "t", [ColumnInfo("x", "int", column_default=None)])
        ])
        after = _make_snapshot([
            TableInfo("public", "t", [ColumnInfo("x", "int", column_default="0")])
        ])
        diff = diff_snapshots(before, after)
        assert diff.changes[0].change_type == ChangeType.COLUMN_DEFAULT_CHANGED

    def test_diff_text_output(self):
        before = _make_snapshot([
            TableInfo("public", "users", [ColumnInfo("id", "integer")])
        ])
        after = _make_snapshot([
            TableInfo("public", "users", [
                ColumnInfo("id", "integer"),
                ColumnInfo("email", "varchar"),
            ])
        ])
        diff = diff_snapshots(before, after)
        text = diff.to_diff_text()
        assert "Column added" in text
        assert "email" in text


# ---------------------------------------------------------------------------
# Demo mode tests
# ---------------------------------------------------------------------------

class TestDemoMode:
    def test_demo_snapshots_are_valid(self):
        before = get_demo_before()
        after = get_demo_after()
        assert len(before.tables) > 0
        assert len(after.tables) > 0
        assert before.source_name == after.source_name

    def test_demo_has_drift(self):
        diff = diff_snapshots(get_demo_before(), get_demo_after())
        assert diff.has_drift
        assert len(diff.changes) >= 5  # we introduced at least 5 changes

    def test_demo_detects_specific_changes(self):
        diff = diff_snapshots(get_demo_before(), get_demo_after())
        change_types = {c.change_type for c in diff.changes}
        assert ChangeType.TABLE_ADDED in change_types
        assert ChangeType.COLUMN_ADDED in change_types
        assert ChangeType.COLUMN_REMOVED in change_types
        assert ChangeType.COLUMN_TYPE_CHANGED in change_types
        assert ChangeType.COLUMN_NULLABLE_CHANGED in change_types

    def test_demo_ddl_output(self):
        before = get_demo_before()
        ddl = before.to_ddl()
        assert "CREATE TABLE" in ddl
        assert "customers" in ddl

    def test_demo_json_output(self):
        import json
        before = get_demo_before()
        data = json.loads(before.to_json())
        assert "tables" in data
        assert len(data["tables"]) > 0


# ---------------------------------------------------------------------------
# Prompt engine tests
# ---------------------------------------------------------------------------

MINIMAL_CONFIG_DICT = {
    "version": "1",
    "model": {"name": "llama3:8b"},
    "prompts": [
        {
            "role": "detect",
            "system": "You are a schema analyst.",
            "user_template": "Source: {source_name}\nDiff: {diff}",
        },
        {
            "role": "diagnose",
            "system": "You are a root-cause expert.",
            "user_template": "Source: {source_name}\nContext: {context}",
        },
    ],
    "sources": [
        {"name": "test", "type": "postgres", "connection_string": "postgresql://localhost/db"},
    ],
}


class TestPromptEngine:
    def test_hydrate_detect(self):
        cfg = TaprootConfig(**MINIMAL_CONFIG_DICT)
        engine = PromptEngine(cfg)
        ctx = PromptContext(source_name="my-db", diff="column added: email")
        prompt = engine.hydrate(PromptRole.DETECT, ctx)
        assert "my-db" in prompt.user
        assert "column added: email" in prompt.user
        assert prompt.system == "You are a schema analyst."

    def test_hydrate_missing_role_raises(self):
        cfg = TaprootConfig(**MINIMAL_CONFIG_DICT)
        engine = PromptEngine(cfg)
        ctx = PromptContext(source_name="x")
        with pytest.raises(KeyError, match="remediate"):
            engine.hydrate(PromptRole.REMEDIATE, ctx)

    def test_available_roles(self):
        cfg = TaprootConfig(**MINIMAL_CONFIG_DICT)
        engine = PromptEngine(cfg)
        assert PromptRole.DETECT in engine.available_roles
        assert PromptRole.DIAGNOSE in engine.available_roles
        assert PromptRole.REMEDIATE not in engine.available_roles

    def test_all_placeholders_filled(self):
        cfg = TaprootConfig(**MINIMAL_CONFIG_DICT)
        engine = PromptEngine(cfg)
        ctx = PromptContext(
            source_name="src",
            schema_before="CREATE TABLE...",
            schema_after="CREATE TABLE...",
            diff="no changes",
            context="postgres 15",
        )
        prompt = engine.hydrate(PromptRole.DETECT, ctx)
        # No unfilled placeholders
        assert "{" not in prompt.user