"""
Postgres schema introspector.

Connects to a Postgres database and extracts schema metadata
(tables, columns, types, constraints) as structured data and DDL.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool = True
    column_default: Optional[str] = None
    character_maximum_length: Optional[int] = None

    def to_ddl(self) -> str:
        parts = [f'"{self.name}"', self.data_type.upper()]
        if self.character_maximum_length:
            parts[-1] = f"{self.data_type.upper()}({self.character_maximum_length})"
        if not self.is_nullable:
            parts.append("NOT NULL")
        if self.column_default:
            parts.append(f"DEFAULT {self.column_default}")
        return " ".join(parts)


@dataclass
class TableInfo:
    schema_name: str
    table_name: str
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    def to_ddl(self) -> str:
        col_lines = [f"    {c.to_ddl()}" for c in self.columns]
        cols = ",\n".join(col_lines)
        return f'CREATE TABLE "{self.schema_name}"."{self.table_name}" (\n{cols}\n);'


@dataclass
class SchemaSnapshot:
    """A point-in-time snapshot of a database's schema."""
    source_name: str
    tables: list[TableInfo] = field(default_factory=list)
    captured_at: Optional[str] = None

    def to_ddl(self) -> str:
        return "\n\n".join(t.to_ddl() for t in self.tables)

    def to_dict(self) -> dict:
        return {
            "source_name": self.source_name,
            "captured_at": self.captured_at,
            "tables": [
                {
                    "schema": t.schema_name,
                    "table": t.table_name,
                    "columns": [
                        {
                            "name": c.name,
                            "type": c.data_type,
                            "nullable": c.is_nullable,
                            "default": c.column_default,
                            "max_length": c.character_maximum_length,
                        }
                        for c in t.columns
                    ],
                }
                for t in self.tables
            ],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


class PostgresIntrospector:
    """
    Connects to Postgres and snapshots schema metadata.

    Requires psycopg2: pip install taproot-rca[postgres]
    """

    INTROSPECT_QUERY = """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE t.table_type = 'BASE TABLE'
            AND c.table_schema = ANY(%s)
        ORDER BY c.table_schema, c.table_name, c.ordinal_position;
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def snapshot(self, schemas: list[str], source_name: str = "") -> SchemaSnapshot:
        """
        Take a point-in-time snapshot of the specified schemas.

        Returns a SchemaSnapshot containing all tables and columns.
        """
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for Postgres introspection. "
                "Install it with: pip install taproot-rca[postgres]"
            )

        from datetime import datetime, timezone

        conn = psycopg2.connect(self.connection_string)
        try:
            with conn.cursor() as cur:
                cur.execute(self.INTROSPECT_QUERY, (schemas,))
                rows = cur.fetchall()
        finally:
            conn.close()

        # Group rows into tables
        tables_map: dict[str, TableInfo] = {}
        for row in rows:
            schema, table, col_name, dtype, nullable, default, max_len = row
            key = f"{schema}.{table}"

            if key not in tables_map:
                tables_map[key] = TableInfo(schema_name=schema, table_name=table)

            tables_map[key].columns.append(
                ColumnInfo(
                    name=col_name,
                    data_type=dtype,
                    is_nullable=(nullable == "YES"),
                    column_default=default,
                    character_maximum_length=max_len,
                )
            )

        return SchemaSnapshot(
            source_name=source_name,
            tables=list(tables_map.values()),
            captured_at=datetime.now(timezone.utc).isoformat(),
        )