-- ============================================================================
-- Taproot-RCA drift simulation
-- ============================================================================
-- Run this AFTER your first `taproot scan` to introduce schema drift.
-- Then run `taproot scan` again to see the changes detected.
--
-- Usage:
--   docker exec -i taproot-postgres psql -U taproot -d ecommerce < dev/drift.sql
--
-- Changes introduced (mirrors the demo mode):
--   1. customers.full_name → customers.name (rename)
--   2. orders.total_amount type widened: numeric(10,2) → numeric(15,2)
--   3. orders.status loses NOT NULL constraint
--   4. order_items gains discount_pct column
--   5. New table: public.shipping_addresses
--   6. analytics.daily_revenue.avg_order_value gets a default value
-- ============================================================================

-- 1. Rename full_name → name
ALTER TABLE public.customers RENAME COLUMN full_name TO name;

-- 2. Widen total_amount precision
ALTER TABLE public.orders ALTER COLUMN total_amount TYPE NUMERIC(15,2);

-- 3. Drop NOT NULL on status
ALTER TABLE public.orders ALTER COLUMN status DROP NOT NULL;

-- 4. Add discount column
ALTER TABLE public.order_items ADD COLUMN discount_pct NUMERIC(5,2) DEFAULT 0;

-- 5. New table
CREATE TABLE public.shipping_addresses (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER      NOT NULL REFERENCES public.customers(id),
    line1       VARCHAR(500) NOT NULL,
    line2       VARCHAR(500),
    city        VARCHAR(100) NOT NULL,
    state       VARCHAR(100),
    postal_code VARCHAR(20)  NOT NULL,
    country     VARCHAR(2)   NOT NULL
);

INSERT INTO public.shipping_addresses (customer_id, line1, city, state, postal_code, country) VALUES
    (1, '123 Main St', 'Torrance', 'CA', '90501', 'US'),
    (2, '456 Oak Ave', 'Austin', 'TX', '73301', 'US');

-- 6. Add default to avg_order_value
ALTER TABLE analytics.daily_revenue ALTER COLUMN avg_order_value SET DEFAULT 0;