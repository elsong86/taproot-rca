"""
Schema diff engine.

Compares two SchemaSnapshots and produces a structured diff
that can be fed to the LLM for drift analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from taproot_rca.connectors.postgres import ColumnInfo, SchemaSnapshot, TableInfo


class ChangeType(str, Enum):
    TABLE_ADDED = "table_added"
    TABLE_REMOVED = "table_removed"
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    COLUMN_NULLABLE_CHANGED = "column_nullable_changed"
    COLUMN_DEFAULT_CHANGED = "column_default_changed"


@dataclass
class SchemaChange:
    """A single detected change between two snapshots."""
    change_type: ChangeType
    table: str
    column: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None

    def describe(self) -> str:
        if self.change_type == ChangeType.TABLE_ADDED:
            return f"+ Table added: {self.table}"
        if self.change_type == ChangeType.TABLE_REMOVED:
            return f"- Table removed: {self.table}"
        if self.change_type == ChangeType.COLUMN_ADDED:
            return f"+ Column added: {self.table}.{self.column} ({self.new_value})"
        if self.change_type == ChangeType.COLUMN_REMOVED:
            return f"- Column removed: {self.table}.{self.column} (was {self.old_value})"
        if self.change_type == ChangeType.COLUMN_TYPE_CHANGED:
            return (
                f"~ Column type changed: {self.table}.{self.column} "
                f"({self.old_value} → {self.new_value})"
            )
        if self.change_type == ChangeType.COLUMN_NULLABLE_CHANGED:
            return (
                f"~ Nullable changed: {self.table}.{self.column} "
                f"({self.old_value} → {self.new_value})"
            )
        if self.change_type == ChangeType.COLUMN_DEFAULT_CHANGED:
            return (
                f"~ Default changed: {self.table}.{self.column} "
                f"({self.old_value} → {self.new_value})"
            )
        return f"? Unknown change on {self.table}"


@dataclass
class SchemaDiff:
    """The full diff between two schema snapshots."""
    source_name: str
    before_captured_at: Optional[str] = None
    after_captured_at: Optional[str] = None
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return len(self.changes) > 0

    @property
    def summary(self) -> str:
        if not self.changes:
            return "No schema drift detected."
        counts: dict[str, int] = {}
        for c in self.changes:
            key = c.change_type.value
            counts[key] = counts.get(key, 0) + 1
        parts = [f"{v} {k.replace('_', ' ')}" for k, v in counts.items()]
        return f"{len(self.changes)} change(s) detected: {', '.join(parts)}"

    def to_diff_text(self) -> str:
        """Render as a human-readable diff string for LLM consumption."""
        if not self.changes:
            return "No changes detected."

        lines = [
            f"Schema Drift Report — {self.source_name}",
            f"Before: {self.before_captured_at or 'unknown'}",
            f"After:  {self.after_captured_at or 'unknown'}",
            f"Total changes: {len(self.changes)}",
            "",
        ]
        for change in self.changes:
            lines.append(f"  {change.describe()}")

        return "\n".join(lines)


def diff_snapshots(before: SchemaSnapshot, after: SchemaSnapshot) -> SchemaDiff:
    """
    Compare two schema snapshots and return all detected changes.
    """
    result = SchemaDiff(
        source_name=before.source_name or after.source_name,
        before_captured_at=before.captured_at,
        after_captured_at=after.captured_at,
    )

    before_tables = {t.full_name: t for t in before.tables}
    after_tables = {t.full_name: t for t in after.tables}

    # Tables removed
    for name in before_tables:
        if name not in after_tables:
            result.changes.append(
                SchemaChange(change_type=ChangeType.TABLE_REMOVED, table=name)
            )

    # Tables added
    for name in after_tables:
        if name not in before_tables:
            result.changes.append(
                SchemaChange(change_type=ChangeType.TABLE_ADDED, table=name)
            )

    # Tables in both — compare columns
    for name in before_tables:
        if name not in after_tables:
            continue

        before_cols = {c.name: c for c in before_tables[name].columns}
        after_cols = {c.name: c for c in after_tables[name].columns}

        # Columns removed
        for col_name in before_cols:
            if col_name not in after_cols:
                result.changes.append(
                    SchemaChange(
                        change_type=ChangeType.COLUMN_REMOVED,
                        table=name,
                        column=col_name,
                        old_value=before_cols[col_name].data_type,
                    )
                )

        # Columns added
        for col_name in after_cols:
            if col_name not in before_cols:
                result.changes.append(
                    SchemaChange(
                        change_type=ChangeType.COLUMN_ADDED,
                        table=name,
                        column=col_name,
                        new_value=after_cols[col_name].data_type,
                    )
                )

        # Columns in both — compare attributes
        for col_name in before_cols:
            if col_name not in after_cols:
                continue

            old = before_cols[col_name]
            new = after_cols[col_name]

            if old.data_type != new.data_type:
                result.changes.append(
                    SchemaChange(
                        change_type=ChangeType.COLUMN_TYPE_CHANGED,
                        table=name,
                        column=col_name,
                        old_value=old.data_type,
                        new_value=new.data_type,
                    )
                )

            if old.is_nullable != new.is_nullable:
                result.changes.append(
                    SchemaChange(
                        change_type=ChangeType.COLUMN_NULLABLE_CHANGED,
                        table=name,
                        column=col_name,
                        old_value=str(old.is_nullable),
                        new_value=str(new.is_nullable),
                    )
                )

            if old.column_default != new.column_default:
                result.changes.append(
                    SchemaChange(
                        change_type=ChangeType.COLUMN_DEFAULT_CHANGED,
                        table=name,
                        column=col_name,
                        old_value=old.column_default or "none",
                        new_value=new.column_default or "none",
                    )
                )

    return result