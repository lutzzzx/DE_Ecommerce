CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS dq_logs;

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id                        TEXT PRIMARY KEY,
    customer_id                     TEXT NOT NULL,
    order_status                    TEXT,
    order_purchase_timestamp        TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id      TEXT NOT NULL,
    order_item_id INT NOT NULL,
    product_id    TEXT NOT NULL,
    seller_id     TEXT,
    price         NUMERIC(12,2),
    freight_value NUMERIC(12,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id              TEXT PRIMARY KEY,
    customer_unique_id       TEXT,
    customer_zip_code_prefix TEXT,
    customer_city            TEXT,
    customer_state           TEXT
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id                     TEXT PRIMARY KEY,
    product_category_name_english  TEXT
);

CREATE TABLE IF NOT EXISTS staging.payments (
    order_id            TEXT NOT NULL,
    payment_sequential  INT NOT NULL,
    payment_type        TEXT,
    payment_installments INT,
    payment_value       NUMERIC(12,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS staging.events (
    event_id          TEXT PRIMARY KEY,
    event_type        TEXT,
    event_timestamp   TIMESTAMP,
    customer_id       TEXT,
    product_id        TEXT,
    session_id        TEXT,
    price              NUMERIC(12,2),
    device             TEXT
);

CREATE TABLE IF NOT EXISTS dq_logs.checkpoint_runs (
    run_id           SERIAL PRIMARY KEY,
    suite_name       TEXT NOT NULL,
    layer            TEXT NOT NULL,
    run_ts           TIMESTAMP DEFAULT now(),
    success          BOOLEAN NOT NULL,
    evaluated_expectations INT,
    successful_expectations INT,
    details_json     JSONB
);
