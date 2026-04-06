"""Tests for the self-healing Git workflow."""

import pytest
from pathlib import Path

from taproot_rca.sql_extractor import (
    extract_migration,
    write_migration_files,
    ExtractedMigration,
)
from taproot_rca.git_ops.healer import _parse_github_url, _safe_branch_name


# ---------------------------------------------------------------------------
# SQL extractor tests
# ---------------------------------------------------------------------------

SAMPLE_REMEDIATION = """
### 1. Forward Migration DDL

```sql
ALTER TABLE public.customers ADD COLUMN phone VARCHAR(20);
```

### 2. Rollback Migration DDL

```sql
ALTER TABLE public.customers DROP COLUMN phone;
```

### 3. Pre-migration Safety Checks

```sql
SELECT COUNT(*) FROM public.customers;
```

### 4. Post-migration Validation Queries

```sql
SELECT column_name FROM information_schema.columns WHERE table_name = 'customers';
```
"""


class TestSqlExtractor:
    def test_extracts_forward_sql(self):
        result = extract_migration(SAMPLE_REMEDIATION)
        assert "ALTER TABLE public.customers ADD COLUMN phone" in result.forward_sql

    def test_extracts_rollback_sql(self):
        result = extract_migration(SAMPLE_REMEDIATION)
        assert "DROP COLUMN phone" in result.rollback_sql

    def test_extracts_pre_checks(self):
        result = extract_migration(SAMPLE_REMEDIATION)
        assert result.pre_checks is not None
        assert "SELECT COUNT" in result.pre_checks

    def test_extracts_post_validation(self):
        result = extract_migration(SAMPLE_REMEDIATION)
        assert result.post_validation is not None
        assert "information_schema" in result.post_validation

    def test_is_complete(self):
        result = extract_migration(SAMPLE_REMEDIATION)
        assert result.is_complete is True

    def test_incomplete_when_missing_rollback(self):
        text = "### Forward migration\n```sql\nALTER TABLE x ADD c INT;\n```"
        result = extract_migration(text)
        assert not result.is_complete

    def test_handles_no_sql_blocks(self):
        text = "Just some prose with no SQL"
        result = extract_migration(text)
        assert result.forward_sql == ""
        assert result.rollback_sql == ""
        assert not result.is_complete


class TestWriteMigrationFiles:
    def test_writes_both_files(self, tmp_path):
        migration = ExtractedMigration(
            forward_sql="ALTER TABLE x ADD c INT;",
            rollback_sql="ALTER TABLE x DROP c;",
        )
        forward, rollback = write_migration_files(
            migration=migration,
            source_name="test-db",
            output_dir=str(tmp_path),
            timestamp="20260101000000",
        )
        assert forward.exists()
        assert rollback.exists()
        assert "ALTER TABLE x ADD c" in forward.read_text()
        assert "ALTER TABLE x DROP c" in rollback.read_text()

    def test_includes_pre_checks_in_forward(self, tmp_path):
        migration = ExtractedMigration(
            forward_sql="ALTER TABLE x ADD c INT;",
            rollback_sql="ALTER TABLE x DROP c;",
            pre_checks="SELECT COUNT(*) FROM x;",
        )
        forward, _ = write_migration_files(
            migration=migration,
            source_name="test-db",
            output_dir=str(tmp_path),
        )
        content = forward.read_text()
        assert "Pre-migration safety checks" in content
        assert "SELECT COUNT(*) FROM x" in content

    def test_filename_convention(self, tmp_path):
        migration = ExtractedMigration(
            forward_sql="x",
            rollback_sql="y",
        )
        forward, rollback = write_migration_files(
            migration=migration,
            source_name="my-db",
            output_dir=str(tmp_path),
            timestamp="20260101000000",
        )
        assert "V20260101000000" in forward.name
        assert ".rollback.sql" in rollback.name


# ---------------------------------------------------------------------------
# Git URL parsing tests
# ---------------------------------------------------------------------------

class TestParseGithubUrl:
    def test_https_url(self):
        result = _parse_github_url("https://github.com/elsong86/taproot-rca")
        assert result == ("elsong86", "taproot-rca")

    def test_https_url_with_git(self):
        result = _parse_github_url("https://github.com/elsong86/taproot-rca.git")
        assert result == ("elsong86", "taproot-rca")

    def test_ssh_url(self):
        result = _parse_github_url("git@github.com:elsong86/taproot-rca.git")
        assert result == ("elsong86", "taproot-rca")

    def test_invalid_url(self):
        result = _parse_github_url("https://gitlab.com/foo/bar")
        assert result is None


class TestSafeBranchName:
    def test_replaces_spaces(self):
        assert _safe_branch_name("my source") == "my-source"

    def test_replaces_special_chars(self):
        assert _safe_branch_name("source/with.dots") == "source-with-dots"

    def test_preserves_alphanumerics(self):
        assert _safe_branch_name("dev_ecommerce-123") == "dev_ecommerce-123"