# Mini Data Warehouse on Greenplum

A faithful replication of the `duck_db` project — the same six-level
investigation into how an analytical database executes and optimizes
large-scale workloads — executed on **Greenplum** (the open-source MPP
database, PostgreSQL-based) instead of DuckDB.

```
Raw CSV/JSONL  (data/raw/)
   │
   ├── CREATE EXTERNAL TABLE ... LOCATION('file://...')   ← query files directly
   ├── COPY ... FROM STDIN                                ← bulk load
   ▼
 Bronze tables   (heap, DISTRIBUTED BY key, + AO range-partitioned orders)
   │
   ▼
 Silver tables   (ELT: cleaning → dedup → typing → business transforms)
   │
   ▼
 Gold star schema (dims + facts + marts, append-optimized, compressed)
   │
   ▼
 Analytics / BI / EXPLAIN ANALYZE
```

## DuckDB → Greenplum, level by level

| DuckDB project | Greenplum project (this folder) |
|---|---|
| in-memory engine, `duckdb.connect()` | psycopg2 connection to a running cluster |
| `read_csv_auto()` | `CREATE EXTERNAL TABLE ... LOCATION ('file://...')` + `COPY ... FROM STDIN` |
| `read_json()` | parsed client-side, streamed via COPY (PXF for production) |
| Parquet flat files | heap tables `DISTRIBUTED BY (key)` |
| hive-partitioned Parquet (`year=/month=`) | `PARTITION BY RANGE (order_date)` (monthly) + AO storage |
| `COPY ... TO PARQUET` | `CREATE TABLE ... WITH (appendonly=true, compresstype=zstd)` |
| Level 4: CSV vs Parquet vs Partitioned | Level 4: CSV-external vs Heap vs AO partitioned |
| `EXPLAIN ANALYZE` (rows/files scanned) | `EXPLAIN ANALYZE` (rows scanned, partitions, slices) |
| data lake = query Parquet files directly | data lake = external tables over raw files + PXF for Parquet |
| Level 6: daily `.parquet` batches | Level 6: daily `.csv` batches, same manifest/dedup logic |

Everything else — schema, data-quality problems, the 9 analytical queries,
the 7 benchmark queries, the incremental pipeline semantics — is ported
as-is.

## Prerequisites

* A running **Greenplum 7** cluster (single-host dev install or a small
  cluster). Greenplum runs on Linux; the fastest dev setup is the Docker
  image described below, or a single-node install with `gpadmin`.
* Python 3.9+ and the client deps:

```bash
pip install -r requirements.txt     # psycopg2-binary, PyYAML
```

## Quick start with Docker (recommended)

The fastest way to get a Greenplum 7 cluster for this project is the
community single-node image `woblerr/greenplum` (1 coordinator + 2
primary segments). Start Docker Desktop first, then **from this folder**:

```bash
docker pull woblerr/greenplum:7.1.0
docker run -d --name gp7 -p 5432:5432 \
  -e GREENPLUM_PASSWORD=gpadmin \
  -e GREENPLUM_DATABASE_NAME=demo \
  -v "$PWD:/gp_project" \
  woblerr/greenplum:7.1.0
```

Flags worth knowing:

* `GREENPLUM_DATABASE_NAME=demo` — do **not** set this to `postgres`:
  gpinitsystem fails to create a database named `postgres` because it
  already exists as the maintenance database. The pipeline connects to
  the `demo` database (via `PGDATABASE=demo`).
* `-v "$PWD:/gp_project"` bind-mounts the project into the container.
  The pipeline runs *inside* the container so the `file://` external
  tables resolve `data/` on the cluster's own filesystem.
* First start initializes the cluster (gpinitsystem); give it 1–3
  minutes, then check readiness:

```bash
docker exec gp7 bash -lc 'source /usr/local/greenplum-db/greenplum_path.sh; PGPASSWORD=gpadmin pg_isready -h localhost -p 5432'
```

Install the client deps once (as root):

```bash
docker exec -u root gp7 bash -lc 'apt-get update && apt-get install -y python3-pip && python3 -m pip install psycopg2-binary PyYAML'
```

Run the full pipeline inside the container:

```bash
docker exec -u gpadmin -w /gp_project gp7 bash -lc '
  source /usr/local/greenplum-db/greenplum_path.sh
  export PGHOST=localhost PGPORT=5432 PGDATABASE=demo PGUSER=gpadmin PGPASSWORD=gpadmin
  python3 run.py                           # generate -> ingest -> ELT -> star schema -> analytics
  python3 run.py --benchmark               # Level 4 benchmark suite (optional)
  python3 src/incremental.py --init --run  # Level 6 incremental pipeline (optional)
'
```

Query the cluster:

```bash
docker exec -it gp7 bash -lc 'source /usr/local/greenplum-db/greenplum_path.sh; PGPASSWORD=gpadmin psql -h localhost -U gpadmin -d demo'
# or the project's ASCII-table shell:
docker exec -it gp7 bash -lc 'source /usr/local/greenplum-db/greenplum_path.sh; export PGHOST=localhost PGDATABASE=demo PGUSER=gpadmin PGPASSWORD=gpadmin; cd /gp_project && python3 src/query.py'
```

Port 5432 is published to the host, so GUI tools (DBeaver, DataGrip,
pgAdmin) can connect to `localhost:5432` with user `gpadmin` / password
`gpadmin`, database `demo`.

### Browser query UI

A tiny Flask app (`web/`) lets you run SQL from the browser — with
sample-query shortcuts and CSV export. It runs in its own container and
reaches Greenplum through the host's published port, so the `gp7`
container stays untouched:

```bash
docker build -t gp7-web web/
docker run -d --name gp7-web -p 8080:8080 \
  -e GPDB_HOST=host.docker.internal -e GPDB_PORT=5432 \
  -e GPDB_DATABASE=demo -e GPDB_USER=gpadmin -e GPDB_PASSWORD=gpadmin \
  gp7-web
# open http://localhost:8080
```

`host.docker.internal` resolves to the host from inside the container,
where port 5432 is published (pg_hba accepts `md5` from any host). If
you run the web app on a machine without Docker, point `GPDB_HOST` at
your Greenplum host instead.

## Connection

Set standard `PG*` env vars, or edit the `database:` section of
`config.yaml`:

```bash
export PGHOST=localhost PGPORT=5432 PGDATABASE=postgres PGUSER=gpadmin PGPASSWORD=
```

The pipeline creates its own schemas (`bronze`, `silver`, `gold`) in that
database, so any database you can write to works.

## Quick start

```bash
python run.py                      # generate -> ingest -> ELT -> star schema -> analytics
python run.py --profile smoke      # quick end-to-end check (default profile is smoke)
python run.py --benchmark          # also run the Level 4 benchmark suite
python src/incremental.py --init --run   # Level 6 incremental pipeline
```

Stages are independently replayable:

```bash
python src/generate_data.py --profile smoke
python src/ingest.py
python src/elt.py
python src/star_schema.py
python src/analytics.py
python src/benchmark.py --quick
```

## Scale profiles (`config.yaml`)

| profile | orders | approx. raw size | use |
|---|---|---|---|
| `smoke` (default) | 100 K | ~40 MB | verify the pipeline |
| `small` | 1 M | ~400 MB | quick experiments |
| `medium` | 10 M | ~4 GB | the "doesn't fit in RAM" case |
| `large` | 100 M | ~40 GB | the "hundreds of millions" case |

Unlike DuckDB, Greenplum has no vectorized in-engine generator, so
`generate_data.py` synthesizes the raw files in Python (same seed, same
distributions, same injected data-quality problems) and the standard
Greenplum bulk-load path (COPY) moves them in. At `medium` this takes a
few minutes; that is the honest price of the "data arrives from a source
system" story.

## Project layout

```
data/                 (generated; gitignored)
├── raw/              CSV/JSONL source files (queried via external tables)
├── silver/           quality report CSV
└── gold/             benchmark results + markdown report

src/
├── generate_data.py  Level 1 — synthetic data (Python, seeded)
├── ingest.py         Level 1 — external tables + COPY into bronze, JSON
├── elt.py            Level 2 — cleaning / dedup / typing -> silver (AO)
├── star_schema.py    Level 3 — dims + facts + gold marts
├── analytics.py      Level 3 — the 9 analytical queries
├── benchmark.py      Level 4 — CSV-ext vs heap vs AO partitioned
├── incremental.py    Level 6 — daily batches, dedup, late arrivals
└── db.py / config.py shared helpers (psycopg2 + EXPLAIN ANALYZE parsing)

sql/analytics.sql     the analytical queries (readable, hand-runnable)
```

Database objects (all schema-qualified):

```
bronze.customers / products / orders / order_items / payments / events   (heap, distributed)
bronze.ext_*                                                             (external, over data/raw)
bronze.orders_partitioned                                                (AO row + zstd, monthly RANGE)
silver.orders / customers / products / order_items / payments            (AO row + zstd)
gold.dim_date / dim_customer / dim_product / dim_payment
gold.fact_orders (AO columnar + zstd, monthly partitions)
gold.fact_order_items (AO columnar + zstd)
gold.daily_sales / customer_metrics / product_metrics                    (Level 5 marts)
gold.incremental_orders / incremental_daily                              (Level 6)
```

## Querying the data

**psql** (the Greenplum shell):

```bash
psql -d postgres -c "SELECT count(*) FROM silver.orders;"
psql -d postgres -c "SELECT * FROM gold.dim_date LIMIT 5;"
psql -d postgres -c "EXPLAIN ANALYZE SELECT status, count(*) FROM bronze.orders GROUP BY 1;"
```

**Python shell** (ASCII tables, no paths needed):

```bash
python src/query.py                          # interactive (end SQL with ';')
python src/query.py -c "SELECT * FROM gold.dim_date LIMIT 3;"
```

## What the levels demonstrate (Greenplum edition)

1. **Ingest** — `file://` external tables read raw CSV without loading;
   `COPY FROM STDIN` bulk-loads into distributed heap tables; JSON events
   are parsed and streamed in; `bronze.orders_partitioned` shows AO +
   zstd + monthly range partitions.
2. **ELT** — window-function dedup (CDC-style order updates, payment retry
   rows), future-date filter, messy status cleanup, email validation,
   NULL/return handling — all parallel SQL across segments.
3. **Star schema** — dims + facts as AO tables; `fact_orders` is columnar
   and range-partitioned; `DISTINCT ON` picks the largest completed payment
   per order (DuckDB's `arg_max`); the 9 analytical queries run unchanged
   in spirit.
4. **Performance** — the same 7 queries against **CSV-external** (single
   master reader, re-parses every run), **heap** (parallel segment scan),
   and **AO partitioned** (compressed + partition pruning). `EXPLAIN
   ANALYZE` metrics: rows scanned, partitions selected, plan slices, wall
   time. Report: `data/gold/benchmark_report.md`.
5. **Data lake** — external tables are the lake view; marts are
   pre-aggregated AO tables. For Parquet/object stores, Greenplum uses
   **PXF** (not covered here; see the PXF docs).
6. **Incremental** — daily CSV batches, SHA-256 manifest, merge +
   dedup into `gold.incremental_orders`, recomputed daily aggregates,
   late-arriving records folded into their true day. Rerun → no-op.

## Known differences from the DuckDB version (deliberate)

* Data is generated in Python, not inside the engine, and everything
  lands in real tables, not Parquet files.
* The `file://` external protocol matches the URL's host against
  `gp_segment_configuration` (an empty host defaults to `localhost`, which
  fails on clusters registered under a different hostname). `ingest.py`
  injects the machine's hostname into the URL, so run the pipeline on the
  same host as the cluster — where Greenplum can see `data/` — or use
  gpfdist/PXF for multi-host or object-store setups.
* GP CSV parsing is untyped — schemas are declared explicitly (no
  `read_csv_auto` sniffing).
* `EXPLAIN ANALYZE` reports segments/slices/partition pruning instead of
  files/bytes scanned.
* INSERT ... ON CONFLICT is heap-only in GP, so the incremental merge uses
  the same window-function dedup as the DuckDB version (portable to any
  layout).
