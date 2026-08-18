Build a Mini Data Warehouse with Greenplum

This is the same brief as the DuckDB version — but executed on Greenplum
(PostgreSQL-based MPP database) instead. Everything that the duck_db
project does with DuckDB is replicated here with Greenplum semantics:

  raw CSV/JSONL  ──►  Greenplum (COPY / external tables)  ──►  bronze
  ──►  ELT  ──►  silver  ──►  star schema  ──►  analytics / benchmark

Build a large-scale e-commerce analytics platform using Greenplum.

Architecture
Raw Data
   │
   ├── customers.csv
   ├── products.csv
   ├── orders.csv
   ├── order_items.csv
   └── payments.csv
          │
          ▼
    Greenplum
          │
          ▼
    Bronze tables  (heap, distributed by key)
          │
          ▼
   Transformations
          │
          ▼
     Silver tables  (append-optimized, compressed)
          │
          ▼
   Star Schema
          │
          ├── fact_orders
          ├── fact_order_items
          ├── dim_customer
          ├── dim_product
          └── dim_date
          │
          ▼
   Analytics / BI
What makes it interesting

Don't make it a 50,000-row toy dataset.

Generate or obtain tens/hundreds of millions of rows.

Then investigate questions like:

What happens when the dataset doesn't fit comfortably in RAM?
How fast is Greenplum reading compressed append-optimized tables?
How much does range partitioning help?
What does predicate pushdown do?
What does projection pushdown do?
How does compression affect storage?
How does Greenplum compare with loading into heap tables?
How does query performance change with different table layouts?
What does EXPLAIN ANALYZE reveal (segments, motion, partition pruning)?
How do external tables (the data-lake view) compare with loaded tables?

Example experiment

Start with:

SELECT
    customer_id,
    SUM(total_amount) AS revenue
FROM orders
WHERE order_date >= DATE '2026-01-01'
GROUP BY customer_id;

Then investigate the execution plan.

EXPLAIN ANALYZE
SELECT
    customer_id,
    SUM(total_amount) AS revenue
FROM orders
WHERE order_date >= DATE '2026-01-01'
GROUP BY customer_id;

Then store the data in a heap table and repeat.

Then store the data in an append-optimized, range-partitioned table and repeat.

Your project isn't just:

"I built an analytics database."

It's:

"I investigated how an analytical database executes and optimizes large-scale workloads."

That's much more valuable.

Make it progressively harder
Level 1 — Ingest

Get raw data into Greenplum.

Learn:

COPY FROM (bulk load)
CREATE EXTERNAL TABLE ... LOCATION ('file://...')   (query files without loading)
JSON ingestion (events.jsonl)
DISTRIBUTED BY, append-optimized storage, range partitions

Then create your first distributed tables.

Level 2 — ELT

Build transformations:

Raw
 ↓
Cleaning
 ↓
Deduplication
 ↓
Type conversion
 ↓
Business transformations
 ↓
Silver tables

Use Greenplum as your transformation engine.

Level 3 — Star schema

Create:

                 dim_customer
                      │
                      │
dim_date ───── fact_orders ───── dim_product
                      │
                      │
                dim_payment

Then implement analytical queries such as:

Revenue by month
Customer lifetime value
Repeat customer rate
Average order value
Product profitability
Cohort retention
Customer segmentation
Revenue by geography

Since you already know SQL, these queries shouldn't be the difficult part.

Level 4 — Performance engineering

This is where I'd spend most of your time.

Create a benchmark suite.

For example:

Query                    CSV-ext      Heap        AO Partitioned
----------------------------------------------------------------
Monthly revenue
Customer revenue
Product revenue
Top customers
12-month retention
Large join
Large aggregation

Record:

execution time
rows scanned (from EXPLAIN ANALYZE)
partitions scanned
memory usage
query plan

Then investigate why performance changes.

Level 5 — Build a data lake

Now make the project much more realistic.

Use:

data/
├── bronze/          (raw source files — queried via external tables)
├── silver/          (cleaned files + quality report)
└── gold/            (benchmark reports, manifests)

and database schemas:

bronze.*   raw tables (heap)
silver.*   cleaned tables (append-optimized)
gold.*     star schema + marts (append-optimized, some columnar)

Have Greenplum query the raw files directly via external tables
(the "file://" protocol) rather than requiring everything to be loaded.
For Parquet/object stores, PXF is the connector.

Level 6 — Incremental processing

This is an excellent challenge.

Pretend you receive:

orders_2026_08_01.csv
orders_2026_08_02.csv
orders_2026_08_03.csv
...

Build an incremental pipeline that:

Detects new files
Processes only new data
Deduplicates records
Updates your analytical tables
Produces daily aggregates
Handles late-arriving records

Now you're doing actual data engineering rather than just SQL analytics
