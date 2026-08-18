"""Level 4 — Performance engineering: benchmark suite.

Runs the same analytical queries against three physical layouts of the
*same* underlying orders data and records how Greenplum's engine responds:

    Layout       Source
    ───────────  ──────────────────────────────────────────────────────
    CSV-ext      bronze.ext_orders            (external table over data/raw)
    Heap         bronze.orders                (heap, DISTRIBUTED BY order_id)
    AO part.     bronze.orders_partitioned    (AO row + zstd, monthly RANGE)

For every query x layout we record:
    wall time          (median of N timed runs)
    rows scanned       (EXPLAIN ANALYZE: rows read by scan operators)
    partitions scanned (Partition Selector row counts, where applicable)
    slices             (plan slices — a sense of parallel work)
    memory             (client process peak RSS)

Results are printed, saved as CSV, and written as a Markdown report
(data/gold/benchmark_report.md).

Usage:
    python src/benchmark.py                 # full suite
    python src/benchmark.py --quick         # 1 run per query (smoke test)
"""
from __future__ import annotations

import argparse
import csv
import statistics
import time
from pathlib import Path

from config import load_config
from db import connect, explain_analyze, fmt_dur, peak_rss_mb

# ── the queries, templated with {orders}, {customers}, {items} ─────────
QUERIES = {
    "monthly_revenue": """
        SELECT EXTRACT(YEAR FROM order_date)::INT AS y,
               EXTRACT(MONTH FROM order_date)::INT AS m,
               SUM(total_amount) AS revenue
        FROM {orders}
        WHERE order_date >= DATE '2025-01-01'
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "customer_revenue": """
        SELECT customer_id, SUM(total_amount) AS revenue
        FROM {orders}
        WHERE order_date >= DATE '2026-01-01'
        GROUP BY customer_id ORDER BY revenue DESC LIMIT 10
    """,
    "product_revenue": """
        SELECT product_id, SUM(amount) AS revenue
        FROM {items}
        GROUP BY product_id ORDER BY revenue DESC LIMIT 10
    """,
    "top_customers": """
        SELECT customer_id, COUNT(*) AS n_orders, SUM(total_amount) AS revenue
        FROM {orders}
        GROUP BY 1 ORDER BY n_orders DESC LIMIT 10
    """,
    "twelve_month_retention": """
        WITH y25 AS (SELECT DISTINCT customer_id FROM {orders}
                     WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'),
             y26 AS (SELECT DISTINCT customer_id FROM {orders}
                     WHERE order_date >= DATE '2026-01-01')
        SELECT COUNT(*) AS retained_12m FROM y25 JOIN y26 USING (customer_id)
    """,
    "large_join": """
        SELECT c.country, SUM(o.total_amount) AS revenue
        FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id
        GROUP BY 1 ORDER BY revenue DESC LIMIT 10
    """,
    "large_aggregation": """
        SELECT COUNT(*) AS n,
               COUNT(DISTINCT customer_id) AS customers,
               COUNT(DISTINCT order_id) AS orders,
               SUM(total_amount) AS revenue
        FROM {orders}
    """,
}


def sources(cfg) -> dict[str, dict[str, str]]:
    """Per-layout table expressions (orders, customers, items)."""
    return {
        "CSV-ext": {
            "orders": "bronze.ext_orders",
            "customers": "bronze.ext_customers",
            "items": "bronze.ext_order_items",
        },
        "Heap": {
            "orders": "bronze.orders",
            "customers": "bronze.customers",
            "items": "bronze.order_items",
        },
        "AO part.": {
            "orders": "bronze.orders_partitioned",
            "customers": "bronze.customers",
            "items": "bronze.order_items",
        },
    }


def run_one(con, sql: str, runs: int) -> dict:
    """Time a query (median of `runs`), plus EXPLAIN ANALYZE metrics."""
    with con.cursor() as cur:
        cur.execute(sql)
        cur.fetchall()                      # warm-up / compile
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        with con.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
        times.append(time.perf_counter() - t0)
    metrics = explain_analyze(con, sql)
    return {"time_s": statistics.median(times), "peak_rss_mb": peak_rss_mb(), **metrics}


def fmt_int(v):
    return f"{v:,}" if isinstance(v, int) else ("-" if v is None else v)


def fmt_mem(v):
    return f"{v} MB" if isinstance(v, int) else "-"


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark CSV-external vs heap vs AO partitioned")
    ap.add_argument("--quick", action="store_true", help="single timed run per query")
    args = ap.parse_args()

    cfg = load_config()
    con = connect(cfg)
    warmup, timed_runs = (0, 1) if args.quick else (cfg.benchmark["warmup_runs"],
                                                    cfg.benchmark["timed_runs"])

    srcs = sources(cfg)
    print("=== Level 4: Benchmark - CSV-ext vs Heap vs AO partitioned ===\n")
    print(f"Queries: {list(QUERIES)}")
    print(f"Timed runs per query x layout: {timed_runs}\n")

    results = []
    for qname, qsql in QUERIES.items():
        print(f"== {qname} " + "=" * max(1, 50 - len(qname)))
        for layout, tables in srcs.items():
            sql = qsql.format(**tables)
            r = run_one(con, sql, timed_runs)
            r.update({"query": qname, "layout": layout})
            results.append(r)
            print(f"  {layout:<10} {fmt_dur(r['time_s']):>9}   "
                  f"rows={fmt_int(r['rows_scanned']):>14}  "
                  f"parts={fmt_int(r['partitions']):>4}  "
                  f"slices={fmt_int(r['slices']):>3}  mem={fmt_mem(r['peak_rss_mb'])}")
        print()

    # ── CSV output ─────────────────────────────────────────────────────
    gold = Path(cfg.data["gold"])
    gold.mkdir(parents=True, exist_ok=True)
    csv_out = gold / "benchmark_results.csv"
    with open(csv_out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["query", "layout", "time_s", "rows_scanned",
                                           "partitions", "slices", "peak_rss_mb"])
        w.writeheader()
        w.writerows(results)
    print(f"Results saved -> {csv_out}")

    # ── Markdown report ────────────────────────────────────────────────
    md = ["# Benchmark report - CSV-external vs Heap vs AO partitioned", "",
          "Same data, three physical layouts, identical queries. Wall time is the "
          "median of runs; rows scanned is the sum of scan-operator `rows` counters "
          "from `EXPLAIN ANALYZE`; partitions scanned comes from Partition Selector "
          "counters; slices are the number of parallel plan slices; memory is the "
          "client process peak RSS.", "",
          "| Query | Layout | Time | Rows scanned | Partitions | Slices | Peak RSS |",
          "|---|---|---|---|---|---|---|"]
    for qname in QUERIES:
        for layout in srcs:
            r = next(x for x in results if x["query"] == qname and x["layout"] == layout)
            md.append(f"| {qname} | {layout} | {fmt_dur(r['time_s'])} "
                      f"| {fmt_int(r['rows_scanned'])} | {fmt_int(r['partitions'])} "
                      f"| {fmt_int(r['slices'])} | {fmt_mem(r['peak_rss_mb'])} |")
    md += ["", "## What to look for", "",
           "* **CSV-ext vs Heap**: external tables stream the raw file through the "
           "master (single reader) and re-parse every row on every query; loaded heap "
           "tables are scanned in parallel across segments and skip the parse step.",
           "* **Heap vs AO part.** : the append-optimized copy is zstd-compressed and "
           "range-partitioned by month, so scans read fewer bytes and a filter on "
           "`order_date` prunes whole partitions (`partitions` drops).",
           "* **Partition pruning**: the `monthly_revenue` / `customer_revenue` "
           "predicates on `order_date` turn into dynamic partition elimination "
           "against `bronze.orders_partitioned`.",
           "* **Slices / segments**: every query is split into slices executed across "
           "segments; joins/aggregations add redistributing Motion nodes. More slices "
           "is not necessarily better — motion costs show up in wall time.",
           "* **Memory**: big joins/aggregations spill to workfiles when they exceed "
           "segment memory; raise `work_mem` or watch `gp_toolkit.gp_workfile_*`.",
           "", "```sql",
           "-- same scan, but partition pruning skips whole month files:",
           "EXPLAIN ANALYZE",
           "SELECT count(*) FROM bronze.orders_partitioned",
           "WHERE order_date >= DATE '2026-01-01';",
           "```", ""]
    md_out = gold / "benchmark_report.md"
    md_out.write_text("\n".join(md), encoding="utf-8")
    print(f"Report saved     -> {md_out}")


if __name__ == "__main__":
    main()
