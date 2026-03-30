"""
Demo mode for Taproot-RCA.

Provides realistic mock schema snapshots with intentional drift
so the full pipeline can be tested without a live database.

The scenario simulates a common data engineering situation:
  - An e-commerce app's schema changed between two snapshots
  - A column was renamed, a type was widened, a new table appeared,
    and a NOT NULL constraint was dropped
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from taproot_rca.connectors.postgres import ColumnInfo, SchemaSnapshot, TableInfo


def get_demo_before() -> SchemaSnapshot:
    """The 'before' snapshot — the known-good baseline schema."""
    return SchemaSnapshot(
        source_name="demo-ecommerce",
        captured_at=(datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
        tables=[
            TableInfo(
                schema_name="public",
                table_name="customers",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False, column_default="nextval('customers_id_seq')"),
                    ColumnInfo(name="email", data_type="character varying", is_nullable=False, character_maximum_length=255),
                    ColumnInfo(name="full_name", data_type="character varying", is_nullable=False, character_maximum_length=200),
                    ColumnInfo(name="created_at", data_type="timestamp with time zone", is_nullable=False, column_default="now()"),
                    ColumnInfo(name="is_active", data_type="boolean", is_nullable=False, column_default="true"),
                ],
            ),
            TableInfo(
                schema_name="public",
                table_name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False, column_default="nextval('orders_id_seq')"),
                    ColumnInfo(name="customer_id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="total_amount", data_type="numeric(10,2)", is_nullable=False),
                    ColumnInfo(name="status", data_type="character varying", is_nullable=False, character_maximum_length=50),
                    ColumnInfo(name="created_at", data_type="timestamp with time zone", is_nullable=False, column_default="now()"),
                ],
            ),
            TableInfo(
                schema_name="public",
                table_name="order_items",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="order_id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="product_name", data_type="character varying", is_nullable=False, character_maximum_length=300),
                    ColumnInfo(name="quantity", data_type="integer", is_nullable=False),
                    ColumnInfo(name="unit_price", data_type="numeric(10,2)", is_nullable=False),
                ],
            ),
            TableInfo(
                schema_name="analytics",
                table_name="daily_revenue",
                columns=[
                    ColumnInfo(name="report_date", data_type="date", is_nullable=False),
                    ColumnInfo(name="total_revenue", data_type="numeric(12,2)", is_nullable=False),
                    ColumnInfo(name="order_count", data_type="integer", is_nullable=False),
                    ColumnInfo(name="avg_order_value", data_type="numeric(10,2)", is_nullable=True),
                ],
            ),
        ],
    )


def get_demo_after() -> SchemaSnapshot:
    """
    The 'after' snapshot — the current schema with drift.

    Changes introduced:
      1. customers.full_name → customers.name (column renamed = remove + add)
      2. orders.total_amount type widened: numeric(10,2) → numeric(15,2)
      3. orders.status lost its NOT NULL constraint
      4. order_items.discount_pct added (new column)
      5. New table: public.shipping_addresses
      6. analytics.daily_revenue.avg_order_value default changed
    """
    return SchemaSnapshot(
        source_name="demo-ecommerce",
        captured_at=datetime.now(timezone.utc).isoformat(),
        tables=[
            TableInfo(
                schema_name="public",
                table_name="customers",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False, column_default="nextval('customers_id_seq')"),
                    ColumnInfo(name="email", data_type="character varying", is_nullable=False, character_maximum_length=255),
                    # DRIFT: full_name → name (rename)
                    ColumnInfo(name="name", data_type="character varying", is_nullable=False, character_maximum_length=200),
                    ColumnInfo(name="created_at", data_type="timestamp with time zone", is_nullable=False, column_default="now()"),
                    ColumnInfo(name="is_active", data_type="boolean", is_nullable=False, column_default="true"),
                ],
            ),
            TableInfo(
                schema_name="public",
                table_name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False, column_default="nextval('orders_id_seq')"),
                    ColumnInfo(name="customer_id", data_type="integer", is_nullable=False),
                    # DRIFT: numeric(10,2) → numeric(15,2)
                    ColumnInfo(name="total_amount", data_type="numeric(15,2)", is_nullable=False),
                    # DRIFT: NOT NULL → nullable
                    ColumnInfo(name="status", data_type="character varying", is_nullable=True, character_maximum_length=50),
                    ColumnInfo(name="created_at", data_type="timestamp with time zone", is_nullable=False, column_default="now()"),
                ],
            ),
            TableInfo(
                schema_name="public",
                table_name="order_items",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="order_id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="product_name", data_type="character varying", is_nullable=False, character_maximum_length=300),
                    ColumnInfo(name="quantity", data_type="integer", is_nullable=False),
                    ColumnInfo(name="unit_price", data_type="numeric(10,2)", is_nullable=False),
                    # DRIFT: new column
                    ColumnInfo(name="discount_pct", data_type="numeric(5,2)", is_nullable=True, column_default="0"),
                ],
            ),
            # DRIFT: new table
            TableInfo(
                schema_name="public",
                table_name="shipping_addresses",
                columns=[
                    ColumnInfo(name="id", data_type="integer", is_nullable=False, column_default="nextval('shipping_addresses_id_seq')"),
                    ColumnInfo(name="customer_id", data_type="integer", is_nullable=False),
                    ColumnInfo(name="line1", data_type="character varying", is_nullable=False, character_maximum_length=500),
                    ColumnInfo(name="line2", data_type="character varying", is_nullable=True, character_maximum_length=500),
                    ColumnInfo(name="city", data_type="character varying", is_nullable=False, character_maximum_length=100),
                    ColumnInfo(name="state", data_type="character varying", is_nullable=True, character_maximum_length=100),
                    ColumnInfo(name="postal_code", data_type="character varying", is_nullable=False, character_maximum_length=20),
                    ColumnInfo(name="country", data_type="character varying", is_nullable=False, character_maximum_length=2),
                ],
            ),
            TableInfo(
                schema_name="analytics",
                table_name="daily_revenue",
                columns=[
                    ColumnInfo(name="report_date", data_type="date", is_nullable=False),
                    ColumnInfo(name="total_revenue", data_type="numeric(12,2)", is_nullable=False),
                    ColumnInfo(name="order_count", data_type="integer", is_nullable=False),
                    # DRIFT: default changed from none → 0
                    ColumnInfo(name="avg_order_value", data_type="numeric(10,2)", is_nullable=True, column_default="0"),
                ],
            ),
        ],
    )