"""Shared Greenplum helpers for the mini data warehouse.

The DuckDB version opened an in-memory engine; here we connect to a
running Greenplum cluster with psycopg2 and set session options that make
behavior predictable across runs (statement_timeout off, work_mem sane).
"""
from __future__ import annotations

import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Iterator

try:
    import psycopg2  # noqa: F401  (lazy: required only for DB-touching scripts)
    import psycopg2.extras
    _HAS_PSYCOPG2 = True
except ImportError:
    psycopg2 = None
    _HAS_PSYCOPG2 = False

# Scan operators whose `rows=` EXPLAIN ANALYZE counter represents rows read
# off storage (the analog of DuckDB's PARQUET_SCAN / CSV_SCAN counters).
SCAN_OPS = ("Seq Scan", "External Scan", "Dynamic Table Scan", "Index Scan",
            "Append-only Scan", "Dynamic Index Scan")


def connect(config):
    """Open a psycopg2 connection to Greenplum with autocommit on.

    Greenplum DDL/DML each run standalone fine with autocommit; it also
    avoids 'current transaction is aborted' cascades in the shell tools.
    """
    if not _HAS_PSYCOPG2:
        print("[error] psycopg2 is required. Install it with:"
              "\n        pip install -r requirements.txt")
        sys.exit(1)
    dsn = config.dsn
    try:
        con = psycopg2.connect(**dsn)
    except psycopg2.OperationalError as e:
        print(f"[error] cannot connect to Greenplum at {dsn['host']}:{dsn['port']}"
              f"/{dsn['dbname']} as {dsn['user']}")
        print(f"        {e}")
        print("        Is the cluster up? Set PGHOST/PGPORT/PGDATABASE/PGUSER/"
              "PGPASSWORD or edit the `database:` section of config.yaml.")
        sys.exit(1)
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET client_min_messages = warning")
    return con


def fmt_dur(seconds: float) -> str:
    if seconds >= 60:
        return f"{seconds/60:.2f} min"
    return f"{seconds*1000:.0f} ms" if seconds < 1 else f"{seconds:.2f} s"


@contextmanager
def timed(label: str = ""):
    """Time a block and yield a result dict: {'label', 'seconds', 'start', 'end'}."""
    result = {"label": label, "seconds": 0.0, "start": None, "end": None}
    t0 = time.perf_counter()
    result["start"] = time.strftime("%H:%M:%S")
    try:
        yield result
    finally:
        result["seconds"] = time.perf_counter() - t0
        result["end"] = time.strftime("%H:%M:%S")


# ── EXPLAIN ANALYZE helpers ───────────────────────────────────────────

def explain_analyze(con, sql: str) -> dict:
    """Run `EXPLAIN ANALYZE <sql>` and extract engine metrics.

    Greenplum's text plan reports, per operator:
        (actual time=..., rows=..., loops=...)
    We sum `rows` across scan operators (rows read off storage) and count
    the slices/segments involved. `partitions` comes from the
    'Partition Selector ...' / 'Dynamic Table Scan (partitions: X/Y)' info.
    """
    out = {"rows_scanned": None, "slices": None, "partitions": None}
    try:
        rows = con.cursor()
        rows.execute(f"EXPLAIN ANALYZE {sql}")
        txt = "\n".join(r[0] for r in rows.fetchall())
    except Exception:
        return out

    scanned = 0
    slices = set()
    partitions = 0
    saw_partition_selector = False
    for line in txt.splitlines():
        if any(op in line for op in SCAN_OPS):
            m = re.search(r"\(actual time=[^)]*rows=(\d+)", line)
            if m:
                scanned += int(m.group(1))
        m = re.search(r"slice(\d+)", line)
        if m:
            slices.add(int(m.group(1)))
        # Partition Selector's rows = number of partitions actually selected
        if "Partition Selector" in line:
            m = re.search(r"\(actual time=[^)]*rows=(\d+)", line)
            if m:
                partitions += int(m.group(1))
                saw_partition_selector = True
    if scanned:
        out["rows_scanned"] = scanned
    if slices:
        out["slices"] = len(slices)
    if saw_partition_selector:
        out["partitions"] = partitions
    return out


def peak_rss_mb() -> int | None:
    """Process peak RSS in MB — Windows API or /proc; None if unavailable."""
    try:
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes

            class PMC(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            get_mem = ctypes.windll.psapi.GetProcessMemoryInfo
            get_mem.argtypes = (wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD)
            get_mem.restype = wintypes.BOOL
            get_mem(ctypes.windll.kernel32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
            return pmc.PeakWorkingSetSize // (1024 * 1024)
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmHWM"):
                    return int(line.split()[1]) // 1024
    except Exception:
        return None
    return None


# ── COPY helpers ──────────────────────────────────────────────────────

def copy_csv(con, table: str, columns: list[str], path, has_header: bool = True) -> int:
    """Stream a CSV file into a Greenplum table via client-side COPY."""
    import csv
    n = 0
    with open(path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        if has_header:
            next(reader, None)
        n = copy_rows(con, table, columns, (tuple(r) for r in reader))
    return n


def copy_rows(con, table: str, columns: list[str], rows: Iterator[tuple],
              chunk: int = 50_000) -> int:
    """Bulk-load `rows` (an iterator of tuples) into `table` via COPY FROM STDIN.

    psycopg2's copy_expert needs a file-like object, so rows are buffered
    into an in-memory CSV text stream and flushed every `chunk` rows. This is
    the Greenplum equivalent of DuckDB's `COPY ... FROM`.
    """
    import csv
    import io

    col_sql = ", ".join(columns)
    sql = f"COPY {table} ({col_sql}) FROM STDIN WITH (FORMAT csv, NULL '')"
    total = 0
    buf = io.StringIO()
    w = csv.writer(buf)
    with con.cursor() as cur:
        for r in rows:
            w.writerow(r)
            total += 1
            if total % chunk == 0:
                buf.seek(0)
                cur.copy_expert(sql, buf)
                buf = io.StringIO()
                w = csv.writer(buf)
        if buf.tell():
            buf.seek(0)
            cur.copy_expert(sql, buf)
    return total
