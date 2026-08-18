"""Interactive Greenplum query shell over the data warehouse schemas.

Usage:
    python src/query.py                 # interactive shell (end a statement with ;)
    echo "SELECT * FROM silver.orders LIMIT 3;" | python src/query.py
    python src/query.py -c "SELECT COUNT(*) FROM gold.fact_orders;"
    python src/query.py my_queries.sql  # run statements from a file
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from config import load_config
from db import connect

LAYERS = ["bronze", "silver", "gold"]


def list_tables(con) -> list[tuple[str, str]]:
    """Return [(schema, table), ...] for every table in the warehouse schemas."""
    with con.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            ORDER BY 1, 2
        """)
        return cur.fetchall()


def print_table(con, sql: str) -> None:
    """Execute sql and print the result as a plain ASCII table."""
    with con.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    if not cols:
        print(f"OK ({sql.strip().split()[0].upper()} executed)")
        return
    data = [[("" if v is None else str(v)) for v in r] for r in rows]
    width = [len(c) for c in cols]
    for r in data:
        for i, v in enumerate(r):
            width[i] = min(max(width[i], len(v)), 60)
    trunc = lambda v: v if len(v) <= 60 else v[:57] + "..."

    def line():
        return "+" + "+".join("-" * (w + 2) for w in width) + "+"

    print(line())
    print("| " + " | ".join(c.ljust(w) for c, w in zip(cols, width)) + " |")
    print(line())
    for r in data:
        print("| " + " | ".join(trunc(v).ljust(w) for v, w in zip(r, width)) + " |")
    print(line())
    print(f"({len(rows)} rows)")


def run_sql(con, sql: str) -> None:
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        return
    head = sql.split()[0].upper()
    if head in ("CREATE", "DROP", "INSERT", "COPY", "TRUNCATE", "ANALYZE", "VACUUM",
                "ALTER", "SET", "GRANT", "COMMENT"):
        with con.cursor() as cur:
            cur.execute(sql)
        print(f"OK ({head})")
    else:
        print_table(con, sql)


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("file", nargs="?", help=".sql file to execute instead of a shell")
    ap.add_argument("-c", "--command", help="run a single SQL statement and exit")
    args = ap.parse_args()

    cfg = load_config()
    con = connect(cfg)

    tables = list_tables(con)
    print(f"Greenplum - {len(tables)} tables registered (schemas bronze/silver/gold)\n")

    # ---- one-shot modes -------------------------------------------------
    if args.command:
        run_sql(con, args.command)
        return
    if args.file:
        sql = Path(args.file).read_text(encoding="utf-8")
        for stmt in sql.split(";"):
            run_sql(con, stmt)
        return

    # ---- interactive shell ---------------------------------------------
    print("Type SQL and end with ';'.  :q or Ctrl+C / Ctrl+D to exit.\n")
    print("Available tables:")
    for schema, table in tables:
        print(f"    {schema}.{table}")
    print()
    buf = ""
    while True:
        try:
            prompt = "greenplum> " if not buf else "       ...> "
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if line.strip() in (":q", ":quit", ":exit"):
            break
        buf += line + "\n"
        if ";" not in buf:
            continue
        for stmt in buf.split(";"):
            try:
                run_sql(con, stmt)
            except Exception as e:
                print(f"ERROR: {e}")
        buf = ""


if __name__ == "__main__":
    main()
