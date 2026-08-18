"""Level 3 — Run the analytical queries over the gold star schema.

Reads sql/analytics.sql, substitutes placeholders with schema-qualified
table names, runs each statement against Greenplum, and prints timing +
a preview of results.

Usage:
    python src/analytics.py
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from config import load_config
from db import connect, fmt_dur


def build_substitutions(cfg) -> dict[str, str]:
    """SQL file placeholder -> schema-qualified table name."""
    return {
        "<gold_fact_orders>": "gold.fact_orders",
        "<gold_fact_order_items>": "gold.fact_order_items",
        "<gold_dim_customer>": "gold.dim_customer",
        "<gold_dim_product>": "gold.dim_product",
        "<gold_dim_date>": "gold.dim_date",
        "<bronze_orders_flat>": "bronze.orders",
    }


def load_queries(cfg) -> list[tuple[str, str]]:
    sql_path = Path(__file__).resolve().parent.parent / "sql" / "analytics.sql"
    text = sql_path.read_text(encoding="utf-8")
    for token, expr in build_substitutions(cfg).items():
        text = text.replace(token, expr)

    # Split on top-level semicolons; capture the "-- N. title ---" headers.
    statements: list[tuple[str, str]] = []
    buffer: list[str] = []
    title = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            m = re.match(r"--\s*(\d+)\.\s*(.*?)\s*-*$", stripped)
            if m:
                title = f"Query {m.group(1)}: {m.group(2)}"
            continue
        buffer.append(line)
        if ";" in line:
            stmt = "\n".join(buffer).strip()
            if stmt:
                statements.append((title or stmt[:60], stmt))
            buffer = []
            title = None
    return statements


def main() -> None:
    cfg = load_config()
    con = connect(cfg)
    print("=== Level 3: Analytical queries ===\n")

    queries = load_queries(cfg)
    if not queries:
        print("  no queries found in sql/analytics.sql")
        return

    total = 0.0
    for title, sql in queries:
        st = time.perf_counter()
        rows = []
        with con.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        elapsed = time.perf_counter() - st
        total += elapsed

        print(f"{title}  [{fmt_dur(elapsed)}]")
        print("  " + "  ".join(f"{c:>14}" for c in cols[:6]))
        for row in rows[:5]:
            print("  " + "  ".join(f"{str(v):>14}" for v in row[:6]))
        if len(rows) > 5:
            print(f"  ... {len(rows) - 5} more rows")
        print()

    print(f"Total: {fmt_dur(total)} across {len(queries)} queries")


if __name__ == "__main__":
    main()
