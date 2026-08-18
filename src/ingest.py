"""Level 1 — Ingest: raw files -> bronze tables in Greenplum.

The DuckDB version demonstrated read_csv_auto()/read_json() and wrote
Parquet. Greenplum's equivalents:

    read_csv_auto()   -> CREATE EXTERNAL TABLE ... LOCATION ('file://...')
                         (query raw files directly, nothing loaded) and
                         COPY ... FROM (bulk load into a distributed table)
    read_json()       -> parsed client-side, streamed in with COPY (PXF is
                         the production connector for JSON/Parquet)
    write parquet     -> heap tables DISTRIBUTED BY a key, plus an
                         append-optimized, compressed, range-partitioned
                         copy of orders (the "partitioning helps?" layout)

Output schemas (all names schema-qualified — the Level 5 lake layout):

    bronze.customers, bronze.products, bronze.orders, bronze.order_items,
    bronze.payments, bronze.events            (heap, distributed)
    bronze.ext_orders, bronze.ext_customers, ...   (external, over raw files)
    bronze.orders_partitioned                 (AO row + zstd + monthly RANGE)

Usage:
    python src/ingest.py            # run generate_data.py first
"""
from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path

import psycopg2

from config import human_bytes, load_config
from db import connect, copy_csv, copy_rows, fmt_dur

# column lists (CSV header order == COPY column order)
CSV_COLUMNS = {
    "customers": ["customer_id", "full_name", "email", "country_code", "country",
                  "region", "city", "signup_date", "birth_date", "gender",
                  "segment", "is_active"],
    "products": ["product_id", "name", "category", "subcategory", "brand",
                 "unit_price", "cost", "weight_kg", "rating", "created_at"],
    "orders": ["order_id", "customer_id", "order_date", "updated_at", "status",
               "channel", "currency", "shipping_country", "total_amount", "n_items"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity",
                    "unit_price", "discount_rate", "amount", "shipped_at"],
    "payments": ["payment_id", "order_id", "payment_date", "amount", "method", "status"],
}

DDL = {
    "customers": """
        CREATE TABLE IF NOT EXISTS bronze.customers (
            customer_id  BIGINT, full_name TEXT, email TEXT,
            country_code TEXT, country TEXT, region TEXT, city TEXT,
            signup_date DATE, birth_date DATE, gender TEXT, segment TEXT,
            is_active BOOLEAN
        ) DISTRIBUTED BY (customer_id)
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS bronze.products (
            product_id INT, name TEXT, category TEXT, subcategory TEXT, brand TEXT,
            unit_price DOUBLE PRECISION, cost DOUBLE PRECISION,
            weight_kg DOUBLE PRECISION, rating DOUBLE PRECISION, created_at DATE
        ) DISTRIBUTED BY (product_id)
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS bronze.orders (
            order_id BIGINT, customer_id BIGINT, order_date DATE, updated_at TIMESTAMP,
            status TEXT, channel TEXT, currency TEXT, shipping_country TEXT,
            total_amount DOUBLE PRECISION, n_items INT
        ) DISTRIBUTED BY (order_id)
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS bronze.order_items (
            order_item_id BIGINT, order_id BIGINT, product_id INT,
            quantity INT, unit_price DOUBLE PRECISION, discount_rate DOUBLE PRECISION,
            amount DOUBLE PRECISION, shipped_at TIMESTAMP
        ) DISTRIBUTED BY (order_id)
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS bronze.payments (
            payment_id BIGINT, order_id BIGINT, payment_date DATE,
            amount DOUBLE PRECISION, method TEXT, status TEXT
        ) DISTRIBUTED BY (order_id)
    """,
    "events": """
        CREATE TABLE IF NOT EXISTS bronze.events (
            event_id BIGINT, event_ts TIMESTAMP, customer_id BIGINT,
            product_id INT, event_type TEXT, session_id TEXT
        ) DISTRIBUTED BY (customer_id)
    """,
}

# external-table column lists (GP has no auto-detect; declare the schema)
EXT_SCHEMAS = {
    "orders": """order_id BIGINT, customer_id BIGINT, order_date DATE,
                 updated_at TIMESTAMP, status TEXT, channel TEXT, currency TEXT,
                 shipping_country TEXT, total_amount DOUBLE PRECISION, n_items INT""",
    "customers": """customer_id BIGINT, full_name TEXT, email TEXT,
                    country_code TEXT, country TEXT, region TEXT, city TEXT,
                    signup_date DATE, birth_date DATE, gender TEXT, segment TEXT,
                    is_active BOOLEAN""",
    "products": """product_id INT, name TEXT, category TEXT, subcategory TEXT,
                   brand TEXT, unit_price DOUBLE PRECISION, cost DOUBLE PRECISION,
                   weight_kg DOUBLE PRECISION, rating DOUBLE PRECISION, created_at DATE""",
    "order_items": """order_item_id BIGINT, order_id BIGINT, product_id INT,
                      quantity INT, unit_price DOUBLE PRECISION,
                      discount_rate DOUBLE PRECISION, amount DOUBLE PRECISION,
                      shipped_at TIMESTAMP""",
    "payments": """payment_id BIGINT, order_id BIGINT, payment_date DATE,
                   amount DOUBLE PRECISION, method TEXT, status TEXT""",
}


def drop(con, name: str) -> None:
    """Drop a relation regardless of type.

    Greenplum 7 models external tables as *foreign* tables, so DROP TABLE
    raises WrongObjectType for them; try DROP FOREIGN TABLE as a fallback.
    """
    with con.cursor() as cur:
        for sql in (f"DROP TABLE IF EXISTS {name} CASCADE",
                    f"DROP FOREIGN TABLE IF EXISTS {name} CASCADE"):
            try:
                cur.execute(sql)
                return
            except psycopg2.errors.WrongObjectType:
                continue


def create_external(con, name: str, path: Path, cols: str) -> None:
    """External table over a raw file (Greenplum's 'query the file' story).

    The `file://` protocol must name a host that has a segment instance (the
    URL host is matched against gp_segment_configuration; an empty host
    defaults to 'localhost', which fails on hosts registered under a
    different hostname). We use the machine's own hostname so the raw files
    resolve on a single-host dev cluster; gpfdist/PXF are the multi-host
    options.
    """
    import socket
    loc = f"file://{socket.gethostname()}{path.as_posix()}"
    with con.cursor() as cur:
        cur.execute(f"""
            CREATE EXTERNAL TABLE {name} ({cols})
            LOCATION ('{loc}') FORMAT 'CSV' (HEADER)
        """)


def load_jsonl(con, table: str, path: Path) -> int:
    """Parse events.jsonl client-side and COPY the columns into bronze.events."""
    cols = ["event_id", "event_ts", "customer_id", "product_id", "event_type", "session_id"]

    def rows():
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                e = json.loads(line)
                yield (e["event_id"], e["event_ts"], e["customer_id"],
                       e["product_id"], e["event_type"], e["session_id"])

    return copy_rows(con, table, cols, rows())


def table_size_mb(con, name: str) -> float:
    with con.cursor() as cur:
        cur.execute(f"SELECT pg_total_relation_size('{name}')")
        return cur.fetchone()[0] / (1024 * 1024)


def main() -> None:
    cfg = load_config()
    con = connect(cfg)
    t0 = time.perf_counter()
    raw = Path(cfg.data["raw"])

    print("=== Level 1: Ingest raw -> bronze tables ===\n")

    for schema in ("bronze", "silver", "gold"):
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # ── 1) external tables over the raw files (query, don't load) ────────
    print("External tables over data/raw (query files directly):")
    for name in EXT_SCHEMAS:
        csv_path = raw / f"{name}.csv"
        if not csv_path.exists():
            continue
        drop(con, f"bronze.ext_{name}")
        create_external(con, f"bronze.ext_{name}", csv_path, EXT_SCHEMAS[name])
        with con.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM bronze.ext_{name}")
            n = cur.fetchone()[0]
        print(f"  bronze.ext_{name:<12} reads {csv_path.name:<18} rows={n:>12,}")

    # ── 2) bronze heap tables + COPY from the CSVs ───────────────────────
    print("\nBronze tables (heap, DISTRIBUTED BY key):")
    for name in ("customers", "products", "orders", "order_items", "payments"):
        csv_path = raw / f"{name}.csv"
        if not csv_path.exists():
            print(f"  [skip] {csv_path.name} not found — run generate_data.py first")
            continue
        drop(con, f"bronze.{name}")
        con.cursor().execute(DDL[name])
        st = time.perf_counter()
        n = copy_csv(con, f"bronze.{name}", CSV_COLUMNS[name], csv_path)
        con.cursor().execute(f"ANALYZE bronze.{name}")
        print(f"  COPY bronze.{name:<13} rows={n:>12,}  "
              f"{table_size_mb(con, f'bronze.{name}'):>8.1f} MB  "
              f"({fmt_dur(time.perf_counter() - st)})")

    # ── 3) JSON events -> bronze.events ──────────────────────────────────
    events_json = raw / "events.jsonl"
    if events_json.exists():
        drop(con, "bronze.events")
        con.cursor().execute(DDL["events"])
        st = time.perf_counter()
        n = load_jsonl(con, "bronze.events", events_json)
        con.cursor().execute("ANALYZE bronze.events")
        print(f"  COPY bronze.events    rows={n:>12,}  "
              f"{table_size_mb(con, 'bronze.events'):>8.1f} MB  "
              f"({fmt_dur(time.perf_counter() - st)})")

    # ── 4) append-optimized, compressed, range-partitioned orders ────────
    with con.cursor() as cur:
        cur.execute("SELECT min(order_date), max(order_date) FROM bronze.orders")
        lo, hi = cur.fetchone()
    if lo is not None:
        lo = date.fromisoformat(str(lo))
        hi = date.fromisoformat(str(hi))
        lo_month = lo.replace(day=1)
        hi_month = add_months(hi.replace(day=1), 1)
        drop(con, "bronze.orders_partitioned")
        con.cursor().execute(f"""
            CREATE TABLE bronze.orders_partitioned (
                order_id BIGINT, customer_id BIGINT, order_date DATE,
                updated_at TIMESTAMP, status TEXT, channel TEXT, currency TEXT,
                shipping_country TEXT, total_amount DOUBLE PRECISION, n_items INT,
                year INT, month INT
            )
            WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
            DISTRIBUTED BY (order_id)
            PARTITION BY RANGE (order_date)
            (START (DATE '{lo_month.isoformat()}') INCLUSIVE
             END (DATE '{hi_month.isoformat()}') EXCLUSIVE
             EVERY (INTERVAL '1 month'))
        """)
        st = time.perf_counter()
        con.cursor().execute(f"""
            INSERT INTO bronze.orders_partitioned
            SELECT order_id, customer_id, order_date, updated_at, status, channel,
                   currency, shipping_country, total_amount, n_items,
                   EXTRACT(YEAR FROM order_date)::INT, EXTRACT(MONTH FROM order_date)::INT
            FROM bronze.orders
        """)
        con.cursor().execute("ANALYZE bronze.orders_partitioned")
        with con.cursor() as cur:
            cur.execute("SELECT count(*) FROM bronze.orders_partitioned")
            n = cur.fetchone()[0]
        print(f"\n  bronze.orders_partitioned  rows={n:>12,}  "
              f"{table_size_mb(con, 'bronze.orders_partitioned'):>8.1f} MB  "
              f"(AO row + zstd, monthly RANGE partitions)  "
              f"({fmt_dur(time.perf_counter() - st)})")

    total = sum(f.stat().st_size for f in raw.iterdir())
    print(f"\nBronze layer complete in {fmt_dur(time.perf_counter() - t0)}. "
          f"Raw source size: {human_bytes(total)}")


def add_months(d: date, n: int) -> date:
    """Add n months to a date (month-end safe)."""
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                      31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return date(year, month, day)


if __name__ == "__main__":
    main()
