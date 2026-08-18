"""Level 2 — ELT: bronze tables -> cleaned, typed, business-ready silver tables.

Greenplum is the transformation engine (parallel across segments):

    Bronze ──► Cleaning ──► Deduplication ──► Type conversion
            ──► Business transforms ──► Silver tables (AO row + zstd)

Data-quality problems handled (injected by generate_data.py):
    orders       duplicate CDC-style rows (keep latest updated_at), future dates,
                 messy status strings, NULL totals
    customers    padded names, malformed emails, NULL signup dates
    products     zero/free prices, NULL costs
    order_items  negative quantities (returns), NULL amounts
    payments     duplicate retry rows

Output (Greenplum schemas):
    silver.orders, silver.customers, silver.products, silver.order_items,
    silver.payments
    data/silver/quality_report.csv    (rows in/out per step)

Usage:
    python src/elt.py
"""
from __future__ import annotations

import csv
import time
from pathlib import Path

from config import human_bytes, load_config
from db import connect, fmt_dur

# DuckDB has no initcap(); Greenplum (PostgreSQL) does.
CAP = "initcap"

DDL = {
    "orders": """
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id BIGINT, customer_id BIGINT, order_date DATE,
            updated_at TIMESTAMP, status TEXT, channel TEXT, currency TEXT,
            shipping_country TEXT, total_amount DOUBLE PRECISION,
            amount_estimated BOOLEAN, n_items INT, year INT, month INT
        ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
          DISTRIBUTED BY (order_id)
    """,
    "customers": """
        CREATE TABLE IF NOT EXISTS silver.customers (
            customer_id BIGINT, full_name TEXT, email TEXT, email_invalid BOOLEAN,
            country_code TEXT, country TEXT, region TEXT, city TEXT,
            signup_date DATE, birth_date DATE, gender TEXT, segment TEXT,
            is_active BOOLEAN
        ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
          DISTRIBUTED BY (customer_id)
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS silver.products (
            product_id INT, name TEXT, category TEXT, subcategory TEXT, brand TEXT,
            unit_price DOUBLE PRECISION, price_estimated BOOLEAN,
            cost DOUBLE PRECISION, weight_kg DOUBLE PRECISION,
            rating DOUBLE PRECISION, created_at DATE, margin_pct DOUBLE PRECISION
        ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
          DISTRIBUTED BY (product_id)
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS silver.order_items (
            order_item_id BIGINT, order_id BIGINT, product_id INT, quantity INT,
            unit_price DOUBLE PRECISION, discount_rate DOUBLE PRECISION,
            amount DOUBLE PRECISION, shipped_at TIMESTAMP
        ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
          DISTRIBUTED BY (order_id)
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS silver.payments (
            payment_id BIGINT, order_id BIGINT, payment_date DATE,
            amount DOUBLE PRECISION, method TEXT, status TEXT
        ) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
          DISTRIBUTED BY (order_id)
    """,
}

EMAIL_OK = (r"lower(btrim(email)) ~ "
            r"'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'")

TRANSFORMS = {
    "orders": """
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC) AS rn
            FROM bronze.orders
        )
        SELECT
            order_id::BIGINT,
            customer_id::BIGINT,
            order_date::DATE,
            updated_at::TIMESTAMP,
            CASE lower(btrim(status))
                WHEN 'shiped'     THEN 'shipped'
                WHEN 'cancell'    THEN 'cancelled'
                WHEN 'cancellled' THEN 'cancelled'
                ELSE lower(btrim(status))
            END AS status,
            lower(btrim(channel)) AS channel,
            upper(currency)       AS currency,
            upper(shipping_country) AS shipping_country,
            COALESCE(total_amount::DOUBLE PRECISION, 0.0) AS total_amount,
            (total_amount IS NULL) AS amount_estimated,
            n_items::INT,
            EXTRACT(YEAR FROM order_date)::INT  AS year,
            EXTRACT(MONTH FROM order_date)::INT AS month
        FROM ranked
        WHERE rn = 1 AND order_date::DATE <= CURRENT_DATE
    """,
    "customers": """
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY customer_id
                                         ORDER BY signup_date DESC NULLS LAST) AS rn
            FROM bronze.customers
        )
        SELECT
            customer_id::BIGINT,
            regexp_replace(btrim(full_name), '\\s+', ' ', 'g') AS full_name,
            CASE WHEN {email} THEN lower(btrim(email)) ELSE NULL END AS email,
            CASE WHEN {email} THEN FALSE ELSE TRUE END AS email_invalid,
            upper(country_code) AS country_code,
            {cap}(country) AS country,
            {cap}(region)  AS region,
            {cap}(city)    AS city,
            signup_date::DATE,
            birth_date::DATE,
            upper(gender)  AS gender,
            COALESCE(lower(segment), 'standard') AS segment,
            COALESCE(is_active, TRUE) AS is_active
        FROM ranked
        WHERE rn = 1
    """.format(email=EMAIL_OK, cap=CAP),
    "products": """
        SELECT
            product_id::INT,
            btrim(name) AS name,
            {cap}(category) AS category,
            {cap}(subcategory) AS subcategory,
            {cap}(brand) AS brand,
            CASE WHEN unit_price > 0 THEN unit_price
                 ELSE ROUND((cost * 1.3)::numeric, 2) END AS unit_price,
            CASE WHEN unit_price > 0 THEN FALSE ELSE TRUE END AS price_estimated,
            COALESCE(cost, ROUND((unit_price * 0.6)::numeric, 2)) AS cost,
            weight_kg::DOUBLE PRECISION,
            rating::DOUBLE PRECISION,
            created_at::DATE,
            ROUND(((unit_price - COALESCE(cost, 0.0)) / NULLIF(unit_price, 0.0) * 100)::numeric, 1)
                AS margin_pct
        FROM bronze.products
    """.format(cap=CAP),
    "order_items": """
        SELECT
            order_item_id::BIGINT,
            order_id::BIGINT,
            product_id::INT,
            quantity::INT,
            unit_price::DOUBLE PRECISION,
            discount_rate::DOUBLE PRECISION,
            COALESCE(amount, quantity * unit_price * (1 - discount_rate)) AS amount,
            shipped_at::TIMESTAMP
        FROM bronze.order_items
        WHERE quantity > 0
    """,
    "payments": """
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY payment_id
                                         ORDER BY payment_date DESC) AS rn
            FROM bronze.payments
        )
        SELECT
            payment_id::BIGINT,
            order_id::BIGINT,
            payment_date::DATE,
            amount::DOUBLE PRECISION,
            lower(method) AS method,
            lower(status) AS status
        FROM ranked
        WHERE rn = 1
    """,
}


def q1(con, sql: str) -> int:
    with con.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def main() -> None:
    cfg = load_config()
    con = connect(cfg)
    t0 = time.perf_counter()
    silver_dir = Path(cfg.data["silver"])
    silver_dir.mkdir(parents=True, exist_ok=True)
    report: list[dict] = []

    print("=== Level 2: ELT bronze -> silver tables ===\n")

    if q1(con, "SELECT count(*) FROM bronze.orders") == 0:
        print("  bronze.orders is empty — run generate_data.py && ingest.py first.")
        return

    for name in DDL:
        st = time.perf_counter()
        n_raw = q1(con, f"SELECT count(*) FROM bronze.{name}")
        report.append({"table": name, "step": "raw rows", "rows": n_raw})

        with con.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS silver.{name} CASCADE")
            cur.execute(DDL[name])
            cur.execute(f"INSERT INTO silver.{name} {TRANSFORMS[name]}")
            cur.execute(f"ANALYZE silver.{name}")
        n_out = q1(con, f"SELECT count(*) FROM silver.{name}")
        report.append({"table": name, "step": "silver output", "rows": n_out})

        removed = n_raw - n_out
        detail = {
            "orders": "dedup + future-date filter",
            "customers": "dedup by signup_date",
            "products": "no rows dropped (0.0 prices estimated)",
            "order_items": "returns (qty <= 0) dropped",
            "payments": "retry dupes dropped",
        }[name]
        print(f"  {name:<12} {n_raw:>12,} -> {n_out:>12,}  "
              f"({removed:>9,} removed: {detail})  [{fmt_dur(time.perf_counter() - st)}]")

    # ── quality report ─────────────────────────────────────────────────
    out = silver_dir / "quality_report.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["table", "step", "rows"])
        w.writeheader()
        w.writerows(report)
    print(f"\n  quality report -> {out}")

    size = sum(f.stat().st_size for f in silver_dir.rglob("*") if f.is_file())
    print(f"Silver layer complete in {fmt_dur(time.perf_counter() - t0)}. "
          f"Local files: {human_bytes(size)} (tables live in Greenplum)")


if __name__ == "__main__":
    main()
