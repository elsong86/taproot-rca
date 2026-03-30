"""
AI-powered documentation generator.

Produces:
  - Data dictionary: table & column descriptions with business context
  - Lineage narratives: where data flows from/to
  - Schema change changelog: documents each drift event

All output is Markdown, designed to be committed alongside code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from taproot_rca.connectors.postgres import SchemaSnapshot
from taproot_rca.ollama_client import OllamaClient
from taproot_rca.schema_diff import SchemaDiff


# ---------------------------------------------------------------------------
# Prompt templates for documentation
# ---------------------------------------------------------------------------

DATA_DICTIONARY_SYSTEM = """\
You are a senior data engineer writing internal documentation for a data catalog.
Your goal is to produce clear, concise, and useful descriptions that help
analysts and engineers understand each table and column at a glance.

Write in a professional but approachable tone. Focus on business meaning,
not just technical details. If you can infer relationships between tables
(e.g. foreign keys, naming conventions), mention them."""

DATA_DICTIONARY_USER = """\
Generate documentation for the following database schema.

Source: {source_name}

=== Schema DDL ===
{schema_ddl}

For each table, provide:
1. A 1-2 sentence description of what the table represents and its business purpose
2. For each column: a brief description of what it stores and any notable constraints

Format your response as Markdown using this structure for each table:

### schema.table_name

table description

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| column_name | type | Yes/No | description |

After all tables, add a "Relationships" section describing how tables connect to each other."""

LINEAGE_SYSTEM = """\
You are a data lineage expert. Given a database schema, infer and document
the likely data flow: where data originates, how it moves through the system,
and what downstream consumers depend on it.

Be specific about upstream sources (applications, APIs, manual entry) and
downstream consumers (dashboards, reports, ML models, exports). Make reasonable
inferences based on table and column naming conventions."""

LINEAGE_USER = """\
Analyze the data lineage for this schema.

Source: {source_name}

=== Schema DDL ===
{schema_ddl}

Provide:
1. A high-level data flow narrative (2-3 paragraphs)
2. For each table, describe:
   - Likely upstream source (where does this data come from?)
   - Update pattern (real-time, batch, event-driven?)
   - Downstream consumers (who/what reads this data?)
3. A simple text-based flow diagram showing data movement

Format as Markdown."""

CHANGELOG_SYSTEM = """\
You are a technical writer documenting schema changes for a data team.
Write clear, actionable changelog entries that help engineers understand
what changed, why it might matter, and what to watch for.

Be specific and concise. Each entry should be useful to someone reviewing
changes at a glance."""

CHANGELOG_USER = """\
Document the following schema drift event for a changelog.

Source: {source_name}
Detected at: {timestamp}

=== Changes ===
{diff_text}

=== Schema Before ===
{schema_before}

=== Schema After ===
{schema_after}

Write a changelog entry in Markdown with:
1. A one-line summary of the change
2. A "Changes" section with a bullet for each change and its impact
3. A "Risk Assessment" section rating overall risk (Low/Medium/High) with explanation
4. An "Action Items" section with recommended follow-ups

Keep it concise — this will be appended to a running changelog file."""


# ---------------------------------------------------------------------------
# Documentation generator
# ---------------------------------------------------------------------------

@dataclass
class DocsConfig:
    """Configuration for documentation generation."""
    docs_dir: str = ".taproot/docs"
    source_name: str = ""


class DocsGenerator:
    """Generates AI-powered documentation from schema snapshots."""

    def __init__(self, client: OllamaClient, docs_dir: str = ".taproot/docs"):
        self.client = client
        self.docs_dir = Path(docs_dir)
        self.docs_dir.mkdir(parents=True, exist_ok=True)

    def generate_data_dictionary(
        self,
        snapshot: SchemaSnapshot,
        stream: bool = True,
    ) -> Path:
        """
        Generate a full data dictionary with AI-written descriptions.

        Returns the path to the generated Markdown file.
        """
        from rich.console import Console
        console = Console()

        ddl = snapshot.to_ddl()

        user_prompt = DATA_DICTIONARY_USER.format(
            source_name=snapshot.source_name,
            schema_ddl=ddl,
        )

        console.print("[bold cyan]Generating data dictionary...[/bold cyan]\n")

        response = self.client.chat(
            system=DATA_DICTIONARY_SYSTEM,
            user=user_prompt,
            stream=stream,
        )

        # Build the markdown file
        header = (
            f"# Data Dictionary — {snapshot.source_name}\n\n"
            f"> Auto-generated by Taproot-RCA on "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
            f">\n"
            f"> Tables: {len(snapshot.tables)} | "
            f"Schemas: {', '.join(sorted(set(t.schema_name for t in snapshot.tables)))}\n\n"
            f"---\n\n"
        )

        content = header + response.content

        out_path = self.docs_dir / self._safe_name(snapshot.source_name) / "data-dictionary.md"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content)

        return out_path

    def generate_lineage(
        self,
        snapshot: SchemaSnapshot,
        stream: bool = True,
    ) -> Path:
        """
        Generate a data lineage narrative.

        Returns the path to the generated Markdown file.
        """
        from rich.console import Console
        console = Console()

        ddl = snapshot.to_ddl()

        user_prompt = LINEAGE_USER.format(
            source_name=snapshot.source_name,
            schema_ddl=ddl,
        )

        console.print("[bold cyan]Generating lineage documentation...[/bold cyan]\n")

        response = self.client.chat(
            system=LINEAGE_SYSTEM,
            user=user_prompt,
            stream=stream,
        )

        header = (
            f"# Data Lineage — {snapshot.source_name}\n\n"
            f"> Auto-generated by Taproot-RCA on "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            f"---\n\n"
        )

        content = header + response.content

        out_path = self.docs_dir / self._safe_name(snapshot.source_name) / "lineage.md"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content)

        return out_path

    def append_changelog(
        self,
        diff: SchemaDiff,
        before: SchemaSnapshot,
        after: SchemaSnapshot,
        stream: bool = False,
    ) -> Path:
        """
        Generate and append a changelog entry for a drift event.

        Uses non-streaming by default since this runs as part of scan
        and we don't want to mix output with the drift analysis.

        Returns the path to the changelog file.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        user_prompt = CHANGELOG_USER.format(
            source_name=after.source_name,
            timestamp=timestamp,
            diff_text=diff.to_diff_text(),
            schema_before=before.to_ddl(),
            schema_after=after.to_ddl(),
        )

        response = self.client.chat(
            system=CHANGELOG_SYSTEM,
            user=user_prompt,
            stream=stream,
        )

        # Build the entry
        entry = (
            f"\n---\n\n"
            f"## {timestamp}\n\n"
            f"{response.content}\n"
        )

        changelog_path = self.docs_dir / self._safe_name(after.source_name) / "CHANGELOG.md"
        changelog_path.parent.mkdir(parents=True, exist_ok=True)

        if changelog_path.exists():
            # Append to existing changelog
            existing = changelog_path.read_text()
            changelog_path.write_text(existing + entry)
        else:
            # Create new changelog
            header = (
                f"# Schema Change Log — {after.source_name}\n\n"
                f"> Maintained automatically by Taproot-RCA\n"
            )
            changelog_path.write_text(header + entry)

        return changelog_path

    @staticmethod
    def _safe_name(name: str) -> str:
        return name.replace("/", "_").replace("\\", "_").replace(" ", "_")