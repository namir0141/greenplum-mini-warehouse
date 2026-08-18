"""Level 3 — Star schema: silver tables -> gold dimensions and facts.

Classic e-commerce star schema in Greenplum (append-optimized tables):

              dim_customer
                   │
                   │
    dim_date ─── fact_orders ─── dim_product
                   │
                   │
             dim_payment        fact_order_items ─── dim_product

Storage choices (Greenplum-specific):
    gold.fact_orders        AO columnar + zstd, monthly RANGE partitions
    gold.fact_order_items   AO columnar + zstd
    gold.dim_*, gold marts  AO row + zstd

Output (schema `gold`):
    dim_date, dim_customer, dim_product, dim_payment
    fact_orders (partitioned), fact_order_items
    daily_sales, customer_metrics, product_metrics   (Level 5 marts)

Usage:
    python src/star_schema.py
"""
from __future__ import annotations

import time
from datetime import date
from pathlib import Path

from config import load_config
from db import connect, fmt_dur


def add_months(d: date, n: int) -> date:
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)


def q1(con, sql: str):
    with con.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def size_mb(con, name: str) -> float:
    return q1(con, f"SELECT pg_total_relation_size('{name}')") / (1024 * 1024)


def main() -> None:
    cfg = load_config()
    con = connect(cfg)
    t0 = time.perf_counter()
    print("=== Level 3: Star schema (silver -> gold) ===\n")

    with con.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute("DROP TABLE IF EXISTS gold.dim_date CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.dim_customer CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.dim_product CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.dim_payment CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.fact_orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.fact_order_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.daily_sales CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.customer_metrics CASCADE")
        cur.execute("DROP TABLE IF EXISTS gold.product_metrics CASCADE")

    # ── dim_date (generate_series — the GP range() analog) ─────────────
    lo, hi = q1(con, "SELECT min(order_date) FROM silver.orders"), \
        q1(con, "SELECT max(order_date) FROM silver.orders")
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.dim_date (
                date_key INT, full_date DATE, year INT, month INT, quarter INT,
                day_of_month INT, week_of_year INT, day_name TEXT, month_name TEXT,
                is_weekend BOOLEAN
            ) DISTRIBUTED BY (date_key)
        """)
        cur.execute(f"""
            INSERT INTO gold.dim_date
            SELECT
                to_char(d, 'YYYYMMDD')::INT AS date_key,
                d::DATE AS full_date,
                EXTRACT(YEAR FROM d)::INT AS year,
                EXTRACT(MONTH FROM d)::INT AS month,
                EXTRACT(QUARTER FROM d)::INT AS quarter,
                EXTRACT(DAY FROM d)::INT AS day_of_month,
                EXTRACT(WEEK FROM d)::INT AS week_of_year,
                to_char(d, 'Day') AS day_name,
                to_char(d, 'Month') AS month_name,
                EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend
            FROM generate_series('{lo}'::date,
                                 '{hi}'::date + INTERVAL '30 days',
                                 INTERVAL '1 day') t(d)
        """)
    n = q1(con, "SELECT count(*) FROM gold.dim_date")
    print(f"  dim_date      rows={n:>8,}  (generate_series, "
          f"{fmt_dur(time.perf_counter() - st)})")

    # ── dim_customer ───────────────────────────────────────────────────
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.dim_customer (
                customer_key BIGINT, full_name TEXT, email TEXT,
                country TEXT, region TEXT, city TEXT, gender TEXT, segment TEXT,
                is_active BOOLEAN, signup_date DATE, birth_date DATE,
                age_years INT, tenure_days INT
            ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
              DISTRIBUTED BY (customer_key)
        """)
        cur.execute("""
            INSERT INTO gold.dim_customer
            SELECT
                customer_id AS customer_key,
                full_name, email, country, region, city, gender, segment, is_active,
                signup_date, birth_date,
                EXTRACT(YEAR FROM age(CURRENT_DATE, birth_date))::INT AS age_years,
                (CURRENT_DATE - signup_date)::INT AS tenure_days
            FROM silver.customers
        """)
        cur.execute("ANALYZE gold.dim_customer")
    n = q1(con, "SELECT count(*) FROM gold.dim_customer")
    print(f"  dim_customer  rows={n:>8,}  ({fmt_dur(time.perf_counter() - st)})")

    # ── dim_product ────────────────────────────────────────────────────
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.dim_product (
                product_key INT, name TEXT, category TEXT, subcategory TEXT,
                brand TEXT, unit_price DOUBLE PRECISION, cost DOUBLE PRECISION,
                margin_pct DOUBLE PRECISION
            ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
              DISTRIBUTED BY (product_key)
        """)
        cur.execute("""
            INSERT INTO gold.dim_product
            SELECT product_id AS product_key, name, category, subcategory, brand,
                   unit_price, cost, margin_pct
            FROM silver.products
        """)
    n = q1(con, "SELECT count(*) FROM gold.dim_product")
    print(f"  dim_product   rows={n:>8,}  ({fmt_dur(time.perf_counter() - st)})")

    # ── dim_payment (static) ───────────────────────────────────────────
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.dim_payment (
                method TEXT, category TEXT
            ) DISTRIBUTED BY (method)
        """)
        cur.execute("""
            INSERT INTO gold.dim_payment VALUES
                ('credit_card', 'card'), ('paypal', 'wallet'),
                ('apple_pay', 'wallet'), ('bank_transfer', 'bank'),
                ('gift_card', 'voucher'), ('cash', 'cash'), ('unknown', 'unknown')
        """)
    print("  dim_payment   rows=7")

    # ── fact_orders (AO columnar, monthly partitions) ──────────────────
    lo = add_months(lo, 0)
    hi = add_months(hi, 1)
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE gold.fact_orders (
                order_id BIGINT, customer_key BIGINT, date_key INT, order_date DATE,
                payment_method TEXT, channel TEXT, currency TEXT, status TEXT,
                n_items INT, revenue DOUBLE PRECISION, gross_revenue DOUBLE PRECISION,
                item_count INT, year INT, month INT
            ) WITH (appendonly=true, orientation=column, compresstype=zstd,
                    compresslevel=5)
              DISTRIBUTED BY (order_id)
              PARTITION BY RANGE (order_date)
              (START (DATE '{lo.isoformat()}') INCLUSIVE
               END (DATE '{hi.isoformat()}') EXCLUSIVE
               EVERY (INTERVAL '1 month'))
        """)
        cur.execute("""
            INSERT INTO gold.fact_orders
            SELECT
                o.order_id,
                o.customer_id AS customer_key,
                to_char(o.order_date, 'YYYYMMDD')::INT AS date_key,
                o.order_date,
                COALESCE(p.method, 'unknown') AS payment_method,
                o.channel, o.currency, o.status, o.n_items,
                COALESCE(i.revenue, 0.0) AS revenue,
                COALESCE(i.gross, 0.0) AS gross_revenue,
                COALESCE(i.item_count, 0) AS item_count,
                EXTRACT(YEAR FROM o.order_date)::INT AS year,
                EXTRACT(MONTH FROM o.order_date)::INT AS month
            FROM silver.orders o
            LEFT JOIN (
                SELECT order_id, SUM(amount) AS revenue,
                       SUM(quantity * unit_price) AS gross, COUNT(*) AS item_count
                FROM silver.order_items GROUP BY 1
            ) i ON o.order_id = i.order_id
            LEFT JOIN (
                -- one payment method per order: largest completed payment
                SELECT DISTINCT ON (order_id) order_id, method
                FROM silver.payments
                WHERE status = 'completed'
                ORDER BY order_id, amount DESC
            ) p ON o.order_id = p.order_id
        """)
        cur.execute("ANALYZE gold.fact_orders")
    n = q1(con, "SELECT count(*) FROM gold.fact_orders")
    # GP7 dropped the pg_partitions view; count leaf partitions via pg_inherits.
    n_parts = q1(con, "SELECT count(*) FROM pg_inherits "
                      "WHERE inhparent = 'gold.fact_orders'::regclass")
    print(f"  fact_orders   rows={n:>8,}  parts={n_parts:>3}  "
          f"{size_mb(con, 'gold.fact_orders'):>8.1f} MB (columnar+zstd)  "
          f"({fmt_dur(time.perf_counter() - st)})")

    # ── fact_order_items ───────────────────────────────────────────────
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.fact_order_items (
                order_item_id BIGINT, order_id BIGINT, customer_key BIGINT,
                date_key INT, product_key INT, quantity INT, unit_price DOUBLE PRECISION,
                discount_rate DOUBLE PRECISION, amount DOUBLE PRECISION
            ) WITH (appendonly=true, orientation=column, compresstype=zstd,
                    compresslevel=5)
              DISTRIBUTED BY (order_id)
        """)
        cur.execute("""
            INSERT INTO gold.fact_order_items
            SELECT
                i.order_item_id, i.order_id,
                o.customer_id AS customer_key,
                to_char(o.order_date, 'YYYYMMDD')::INT AS date_key,
                i.product_id AS product_key,
                i.quantity, i.unit_price, i.discount_rate, i.amount
            FROM silver.order_items i
            JOIN silver.orders o ON i.order_id = o.order_id
        """)
        cur.execute("ANALYZE gold.fact_order_items")
    n = q1(con, "SELECT count(*) FROM gold.fact_order_items")
    print(f"  fact_order_items rows={n:>8,}  "
          f"{size_mb(con, 'gold.fact_order_items'):>8.1f} MB (columnar+zstd)  "
          f"({fmt_dur(time.perf_counter() - st)})")

    # ── Gold marts (Level 5) ───────────────────────────────────────────
    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.daily_sales (
                date_key INT, full_date DATE, orders BIGINT, revenue DOUBLE PRECISION,
                items BIGINT, customers BIGINT, year INT, month INT
            ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
              DISTRIBUTED BY (date_key)
        """)
        cur.execute("""
            INSERT INTO gold.daily_sales
            SELECT date_key, full_date, COUNT(*) AS orders, SUM(revenue) AS revenue,
                   SUM(item_count) AS items,
                   COUNT(DISTINCT customer_key) AS customers,
                   EXTRACT(YEAR FROM full_date)::INT AS year,
                   EXTRACT(MONTH FROM full_date)::INT AS month
            FROM gold.fact_orders f
            JOIN gold.dim_date d USING (date_key)
            WHERE status != 'cancelled'
            GROUP BY 1, 2
        """)
    n = q1(con, "SELECT count(*) FROM gold.daily_sales")
    print(f"  daily_sales   rows={n:>8,}  (Level 5 mart)  "
          f"({fmt_dur(time.perf_counter() - st)})")

    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.customer_metrics (
                customer_key BIGINT, order_count BIGINT, total_revenue DOUBLE PRECISION,
                avg_order_value DOUBLE PRECISION, total_items BIGINT,
                first_order_date DATE, last_order_date DATE,
                days_since_last_order INT
            ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
              DISTRIBUTED BY (customer_key)
        """)
        cur.execute("""
            INSERT INTO gold.customer_metrics
            SELECT customer_key,
                   COUNT(*) AS order_count,
                   SUM(revenue) AS total_revenue,
                   ROUND((SUM(revenue) / COUNT(*))::numeric, 2) AS avg_order_value,
                   SUM(item_count) AS total_items,
                   MIN(full_date) AS first_order_date,
                   MAX(full_date) AS last_order_date,
                   (CURRENT_DATE - MAX(full_date))::INT AS days_since_last_order
            FROM gold.fact_orders f
            JOIN gold.dim_date d USING (date_key)
            WHERE status != 'cancelled'
            GROUP BY 1
        """)
    n = q1(con, "SELECT count(*) FROM gold.customer_metrics")
    print(f"  customer_metrics rows={n:>8,}  (Level 5 mart)  "
          f"({fmt_dur(time.perf_counter() - st)})")

    st = time.perf_counter()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE gold.product_metrics (
                product_key INT, category TEXT, brand TEXT, units_sold BIGINT,
                revenue DOUBLE PRECISION, gross_profit DOUBLE PRECISION,
                margin_pct DOUBLE PRECISION
            ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
              DISTRIBUTED BY (product_key)
        """)
        cur.execute("""
            INSERT INTO gold.product_metrics
            SELECT p.product_key, p.category, p.brand,
                   COUNT(*) AS units_sold,
                   SUM(i.amount) AS revenue,
                   ROUND((SUM(i.amount) - SUM(i.quantity * p.cost))::numeric, 2) AS gross_profit,
                   ROUND((100.0 * (SUM(i.amount) - SUM(i.quantity * p.cost))
                          / NULLIF(SUM(i.amount), 0))::numeric, 1) AS margin_pct
            FROM gold.fact_order_items i
            JOIN gold.dim_product p USING (product_key)
            GROUP BY 1, 2, 3
        """)
    n = q1(con, "SELECT count(*) FROM gold.product_metrics")
    print(f"  product_metrics rows={n:>8,}  (Level 5 mart)  "
          f"({fmt_dur(time.perf_counter() - st)})")

    total = sum(size_mb(con, f"gold.{t}")
                for t in ("fact_orders", "fact_order_items", "dim_date",
                          "dim_customer", "dim_product", "dim_payment",
                          "daily_sales", "customer_metrics", "product_metrics"))
    print(f"\nGold layer complete in {fmt_dur(time.perf_counter() - t0)}. "
          f"Total gold size: {total:.1f} MB")


if __name__ == "__main__":
    main()
