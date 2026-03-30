"""
Snapshot storage.

Saves schema snapshots to disk as JSON and retrieves the most recent
snapshot for a given source to enable before/after diffing.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from taproot_rca.connectors.postgres import (
    ColumnInfo,
    SchemaSnapshot,
    TableInfo,
)


class SnapshotStore:
    """
    Manages schema snapshots on the local filesystem.

    Directory layout:
        {snapshot_dir}/{source_name}/{timestamp}.json
    """

    def __init__(self, snapshot_dir: str = ".taproot/snapshots"):
        self.base_dir = Path(snapshot_dir)

    def save(self, snapshot: SchemaSnapshot) -> Path:
        """Save a snapshot to disk. Returns the file path."""
        source_dir = self.base_dir / self._safe_name(snapshot.source_name)
        source_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = source_dir / f"{timestamp}.json"
        path.write_text(snapshot.to_json())
        return path

    def get_latest(self, source_name: str) -> Optional[SchemaSnapshot]:
        """Load the most recent snapshot for a source, or None."""
        source_dir = self.base_dir / self._safe_name(source_name)
        if not source_dir.exists():
            return None

        files = sorted(source_dir.glob("*.json"), reverse=True)
        if not files:
            return None

        return self._load_snapshot(files[0])

    def list_snapshots(self, source_name: str) -> list[Path]:
        """List all snapshot files for a source, newest first."""
        source_dir = self.base_dir / self._safe_name(source_name)
        if not source_dir.exists():
            return []
        return sorted(source_dir.glob("*.json"), reverse=True)

    def _load_snapshot(self, path: Path) -> SchemaSnapshot:
        """Deserialize a snapshot JSON file back into a SchemaSnapshot."""
        data = json.loads(path.read_text())

        tables = []
        for t in data.get("tables", []):
            columns = [
                ColumnInfo(
                    name=c["name"],
                    data_type=c["type"],
                    is_nullable=c.get("nullable", True),
                    column_default=c.get("default"),
                    character_maximum_length=c.get("max_length"),
                )
                for c in t.get("columns", [])
            ]
            tables.append(
                TableInfo(
                    schema_name=t["schema"],
                    table_name=t["table"],
                    columns=columns,
                )
            )

        return SchemaSnapshot(
            source_name=data.get("source_name", ""),
            tables=tables,
            captured_at=data.get("captured_at"),
        )

    @staticmethod
    def _safe_name(name: str) -> str:
        """Sanitize a source name for use as a directory name."""
        return name.replace("/", "_").replace("\\", "_").replace(" ", "_")