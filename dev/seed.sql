-- ============================================================================
-- Taproot-RCA development seed data
-- ============================================================================
-- This creates the same e-commerce schema used in demo mode so you can
-- test live scanning against a real Postgres instance.
--
-- Baseline schema (the "before" state). After your first `taproot scan`,
-- run dev/drift.sql to introduce schema drift and scan again.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- public.customers
-- ---------------------------------------------------------------------------
CREATE TABLE public.customers (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL,
    full_name   VARCHAR(200) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_active   BOOLEAN      NOT NULL DEFAULT true
);

INSERT INTO public.customers (email, full_name) VALUES
    ('alice@example.com', 'Alice Johnson'),
    ('bob@example.com', 'Bob Smith'),
    ('carol@example.com', 'Carol Williams');

-- ---------------------------------------------------------------------------
-- public.orders
-- ---------------------------------------------------------------------------
CREATE TABLE public.orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INTEGER      NOT NULL REFERENCES public.customers(id),
    total_amount NUMERIC(10,2) NOT NULL,
    status       VARCHAR(50)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO public.orders (customer_id, total_amount, status) VALUES
    (1, 150.00, 'completed'),
    (1, 89.99, 'completed'),
    (2, 245.50, 'pending'),
    (3, 32.00, 'shipped');

-- ---------------------------------------------------------------------------
-- public.order_items
-- ---------------------------------------------------------------------------
CREATE TABLE public.order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INTEGER      NOT NULL REFERENCES public.orders(id),
    product_name VARCHAR(300) NOT NULL,
    quantity     INTEGER      NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL
);

INSERT INTO public.order_items (order_id, product_name, quantity, unit_price) VALUES
    (1, 'Wireless Headphones', 1, 79.99),
    (1, 'USB-C Cable', 2, 12.99),
    (2, 'Mechanical Keyboard', 1, 89.99),
    (3, 'Standing Desk Mat', 1, 45.00),
    (3, 'Monitor Light Bar', 1, 59.99),
    (4, 'Webcam Cover', 4, 8.00);

-- ---------------------------------------------------------------------------
-- analytics.daily_revenue
-- ---------------------------------------------------------------------------
CREATE TABLE analytics.daily_revenue (
    report_date     DATE          NOT NULL PRIMARY KEY,
    total_revenue   NUMERIC(12,2) NOT NULL,
    order_count     INTEGER       NOT NULL,
    avg_order_value NUMERIC(10,2)
);

INSERT INTO analytics.daily_revenue (report_date, total_revenue, order_count, avg_order_value) VALUES
    ('2026-03-28', 239.99, 2, 120.00),
    ('2026-03-29', 245.50, 1, 245.50),
    ('2026-03-30', 32.00, 1, 32.00);