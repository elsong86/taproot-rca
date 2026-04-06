"""
SQL extractor.

The remediate stage of the pipeline produces a markdown response with
forward migration DDL, rollback DDL, and validation queries. This module
extracts those SQL blocks and writes them as proper migration files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class ExtractedMigration:
    """Forward and rollback SQL extracted from an LLM remediation response."""
    forward_sql: str
    rollback_sql: str
    pre_checks: Optional[str] = None
    post_validation: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        return bool(self.forward_sql and self.rollback_sql)


# Markdown code block pattern: ```sql ... ```
_SQL_BLOCK_PATTERN = re.compile(
    r"```(?:sql)?\s*\n(.*?)\n```",
    re.DOTALL | re.IGNORECASE,
)


def extract_migration(remediation_text: str) -> ExtractedMigration:
    """
    Parse the remediation stage output to extract SQL migration blocks.

    The LLM is prompted to return four sections:
      1. Forward migration DDL
      2. Rollback migration DDL
      3. Pre-migration safety checks
      4. Post-migration validation queries

    This function locates each section by its header and extracts the
    SQL code block that follows it.
    """
    sections = _split_into_sections(remediation_text)

    return ExtractedMigration(
        forward_sql=_first_sql_block(sections.get("forward", "")),
        rollback_sql=_first_sql_block(sections.get("rollback", "")),
        pre_checks=_first_sql_block(sections.get("pre_check", "")) or None,
        post_validation=_first_sql_block(sections.get("post_validation", "")) or None,
    )


def write_migration_files(
    migration: ExtractedMigration,
    source_name: str,
    output_dir: str = "migrations",
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    """
    Write the forward and rollback migration to files.

    Returns a tuple of (forward_path, rollback_path).

    Filenames follow the convention used by Flyway/Alembic:
        V{timestamp}__{source}_drift.sql
        V{timestamp}__{source}_drift.rollback.sql
    """
    if not timestamp:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    out_dir = Path(output_dir) / _safe_name(source_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"V{timestamp}__{_safe_name(source_name)}_drift"
    forward_path = out_dir / f"{base}.sql"
    rollback_path = out_dir / f"{base}.rollback.sql"

    # Forward migration
    forward_content = _build_migration_file(
        title=f"Forward migration for {source_name}",
        timestamp=timestamp,
        sql=migration.forward_sql,
        pre_checks=migration.pre_checks,
        post_validation=migration.post_validation,
    )
    forward_path.write_text(forward_content)

    # Rollback migration
    rollback_content = _build_migration_file(
        title=f"Rollback migration for {source_name}",
        timestamp=timestamp,
        sql=migration.rollback_sql,
    )
    rollback_path.write_text(rollback_content)

    return forward_path, rollback_path


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _split_into_sections(text: str) -> dict[str, str]:
    """
    Split the remediation text into sections by detecting common headers.

    The LLM doesn't always use the exact same wording, so we match on
    keywords. This is intentionally fuzzy.
    """
    # Normalize line endings
    text = text.replace("\r\n", "\n")

    # Define section markers — the LLM may use any of these phrasings
    markers = {
        "forward": [
            "forward migration",
            "forward sql",
            "1. forward",
            "forward ddl",
            "migration script",
        ],
        "rollback": [
            "rollback migration",
            "rollback sql",
            "2. rollback",
            "rollback ddl",
            "rollback script",
        ],
        "pre_check": [
            "pre-migration",
            "pre migration",
            "safety check",
            "3. pre",
            "pre check",
        ],
        "post_validation": [
            "post-migration",
            "post migration",
            "validation quer",
            "4. post",
            "post validation",
        ],
    }

    # Find the position of each section header in the text
    positions: list[tuple[int, str]] = []
    lower = text.lower()

    for section_name, keywords in markers.items():
        for keyword in keywords:
            idx = lower.find(keyword)
            if idx >= 0:
                positions.append((idx, section_name))
                break  # only take the first match per section

    # Sort by position
    positions.sort()

    # Extract text between consecutive headers
    sections: dict[str, str] = {}
    for i, (start, name) in enumerate(positions):
        end = positions[i + 1][0] if i + 1 < len(positions) else len(text)
        sections[name] = text[start:end]

    return sections


def _first_sql_block(text: str) -> str:
    """Extract the first ```sql ... ``` code block from a section."""
    if not text:
        return ""
    match = _SQL_BLOCK_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return ""


def _build_migration_file(
    title: str,
    timestamp: str,
    sql: str,
    pre_checks: Optional[str] = None,
    post_validation: Optional[str] = None,
) -> str:
    """Build a complete migration file with header comments."""
    lines = [
        f"-- ============================================================================",
        f"-- {title}",
        f"-- Generated by Taproot-RCA on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"-- Migration ID: {timestamp}",
        f"-- ============================================================================",
        f"",
    ]

    if pre_checks:
        lines.extend([
            "-- Pre-migration safety checks",
            "-- Run these manually before applying the migration:",
            "",
            "/*",
            pre_checks,
            "*/",
            "",
        ])

    lines.append("BEGIN;")
    lines.append("")
    lines.append(sql)
    lines.append("")
    lines.append("COMMIT;")
    lines.append("")

    if post_validation:
        lines.extend([
            "-- Post-migration validation",
            "-- Run these after applying the migration:",
            "",
            "/*",
            post_validation,
            "*/",
            "",
        ])

    return "\n".join(lines)


def _safe_name(name: str) -> str:
    """Sanitize a source name for use in filenames."""
    return name.replace("/", "_").replace("\\", "_").replace(" ", "_").replace(".", "_")