"""Tests for documentation generator."""

import pytest
from pathlib import Path

from taproot_rca.docs_generator import (
    DocsGenerator,
    DATA_DICTIONARY_SYSTEM,
    DATA_DICTIONARY_USER,
    LINEAGE_SYSTEM,
    LINEAGE_USER,
    CHANGELOG_SYSTEM,
    CHANGELOG_USER,
)
from taproot_rca.demo import get_demo_before, get_demo_after
from taproot_rca.schema_diff import diff_snapshots


class TestPromptTemplates:
    """Verify prompt templates have the expected placeholders."""

    def test_data_dictionary_placeholders(self):
        assert "{source_name}" in DATA_DICTIONARY_USER
        assert "{schema_ddl}" in DATA_DICTIONARY_USER

    def test_lineage_placeholders(self):
        assert "{source_name}" in LINEAGE_USER
        assert "{schema_ddl}" in LINEAGE_USER

    def test_changelog_placeholders(self):
        assert "{source_name}" in CHANGELOG_USER
        assert "{timestamp}" in CHANGELOG_USER
        assert "{diff_text}" in CHANGELOG_USER
        assert "{schema_before}" in CHANGELOG_USER
        assert "{schema_after}" in CHANGELOG_USER


class TestDocsGeneratorPaths:
    """Test that the docs generator creates correct directory structures."""

    def test_safe_name(self):
        assert DocsGenerator._safe_name("my-source") == "my-source"
        assert DocsGenerator._safe_name("my source") == "my_source"
        assert DocsGenerator._safe_name("path/to/source") == "path_to_source"

    def test_docs_dir_created(self, tmp_path):
        from unittest.mock import MagicMock
        mock_client = MagicMock()
        docs_dir = tmp_path / "docs"

        gen = DocsGenerator(client=mock_client, docs_dir=str(docs_dir))
        assert docs_dir.exists()

    def test_changelog_creates_new_file(self, tmp_path):
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "## Test changelog entry\n\nSomething changed."
        mock_client.chat.return_value = mock_response

        gen = DocsGenerator(client=mock_client, docs_dir=str(tmp_path))

        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        path = gen.append_changelog(diff, before, after, stream=False)
        assert path.exists()
        content = path.read_text()
        assert "Schema Change Log" in content
        assert "Test changelog entry" in content

    def test_changelog_appends_to_existing(self, tmp_path):
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "Entry content"
        mock_client.chat.return_value = mock_response

        gen = DocsGenerator(client=mock_client, docs_dir=str(tmp_path))

        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        # First entry
        gen.append_changelog(diff, before, after, stream=False)
        # Second entry
        path = gen.append_changelog(diff, before, after, stream=False)

        content = path.read_text()
        # Should have the header once and two entries
        assert content.count("Schema Change Log") == 1
        assert content.count("Entry content") == 2