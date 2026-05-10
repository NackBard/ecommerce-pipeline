CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE raw.events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id UUID NOT NULL,
    payload JSONB NOT NULL,
    create_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE mart.daily_revenue (
    date DATE PRIMARY KEY,
    orders_count INT NOT NULL DEFAULT 0,
    revenue NUMERIC(12, 2) NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(12,2) NOT NULL DEFAULT 0
);

CREATE TABLE mart.conversion (
    date DATE PRIMARY KEY,
    views_count INT NOT NULL DEFAULT 0,
    orders_count INT NOT NULL DEFAULT 0,
    conversion_rate NUMERIC(5, 2)
);

CREATE TABLE mart.cohort_ltv (
    cohort_month DATE NOT NULL,
    order_month DATE NOT NULL,
    users_count INT NOT NULL DEFAULT 0,
    revenue NUMERIC(12,2) NOT NULL DEFAULT 0,
    PRIMARY KEY(cohort_month, order_month)
);