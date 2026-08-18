"""Minimal browser UI for running SQL against the Greenplum demo database.

Run inside Docker (see Dockerfile) or locally:
    pip install -r requirements.txt
    python app.py
Then open http://localhost:8080
"""

import os
import time
import traceback

import psycopg2
from flask import Flask, render_template_string, request

app = Flask(__name__)

DB = dict(
    host=os.environ.get("GPDB_HOST", "localhost"),
    port=int(os.environ.get("GPDB_PORT", "5432")),
    dbname=os.environ.get("GPDB_DATABASE", "demo"),
    user=os.environ.get("GPDB_USER", "gpadmin"),
    password=os.environ.get("GPDB_PASSWORD", "gpadmin"),
)

DEFAULT_SQL = """SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1, 2;"""

SAMPLES = [
    ("Tables", "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1, 2;"),
    ("Row counts per layer", "SELECT 'bronze.orders' AS tbl, count(*) FROM bronze.orders UNION ALL SELECT 'silver.orders', count(*) FROM silver.orders UNION ALL SELECT 'gold.fact_orders', count(*) FROM gold.fact_orders;"),
    ("Orders by status", "SELECT status, count(*) FROM silver.orders GROUP BY 1 ORDER BY 2 DESC;"),
    ("Daily sales", "SELECT * FROM gold.daily_sales ORDER BY full_date DESC LIMIT 10;"),
    ("Top customers", "SELECT segment, customers, total_revenue FROM gold.customer_metrics ORDER BY total_revenue DESC LIMIT 10;"),
    ("Query plan", "EXPLAIN ANALYZE SELECT status, count(*) FROM bronze.orders GROUP BY 1;"),
]

PAGE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Greenplum Query UI</title>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; margin: 24px auto; max-width: 1100px; background: #f5f6f8; color: #222; }
    h1 { font-size: 20px; margin: 0 0 4px; }
    .sub { color: #666; font-size: 13px; margin-bottom: 14px; }
    textarea { width: 100%; box-sizing: border-box; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px;
               min-height: 100px; padding: 10px; border: 1px solid #ccc; border-radius: 6px; background: #fff; }
    .bar { margin: 10px 0; }
    button { font-size: 13px; padding: 6px 14px; border-radius: 6px; border: 1px solid #aaa; background: #fff; cursor: pointer; }
    button.run { background: #1a6fb0; border-color: #1a6fb0; color: #fff; font-weight: 600; }
    button:hover { filter: brightness(1.06); }
    .samples { margin: 4px 0 10px; }
    .samples button { padding: 3px 9px; font-size: 12px; margin-right: 6px; }
    .meta { color: #666; font-size: 12px; margin: 10px 0 4px; }
    .error { background: #fdecea; border: 1px solid #f5c6c2; color: #8a1f11; padding: 10px 12px; border-radius: 6px;
             font-family: ui-monospace, monospace; font-size: 12px; white-space: pre-wrap; }
    table { border-collapse: collapse; width: 100%; background: #fff; font-size: 13px; }
    th, td { border: 1px solid #ddd; padding: 6px 10px; text-align: left; vertical-align: top; }
    th { background: #eef1f5; position: sticky; top: 0; }
    tr:nth-child(even) { background: #fafbfc; }
    .wrap { overflow-x: auto; max-height: 70vh; }
  </style>
</head>
<body>
  <h1>Greenplum Query UI</h1>
  <div class="sub">{{ db.host }}:{{ db.port }} / {{ db.dbname }} ({{ db.user }}) &middot; Greenplum 7 in Docker</div>
  <form method="post">
    <textarea name="sql" spellcheck="false">{{ sql }}</textarea>
    <div class="samples">
      {% for label, _ in samples %}<button type="button" onclick="setSql({{ loop.index0 }})">{{ label }}</button>{% endfor %}
    </div>
    <div class="bar">
      <button class="run" type="submit">Run</button>
      <button type="button" onclick="document.querySelector('textarea').value=''">Clear</button>
      <button type="submit" formaction="/export" formmethod="get">Export CSV</button>
    </div>
  </form>
  {% if error %}<div class="error">{{ error }}</div>{% endif %}
  {% if cols %}
    <div class="meta">{{ rows|length }} row{% if rows|length != 1 %}s{% endif %} &middot; {{ cols|length }} col{% if cols|length != 1 %}s{% endif %} &middot; {{ ms }} ms</div>
    <div class="wrap"><table>
      <thead><tr>{% for c in cols %}<th>{{ c }}</th>{% endfor %}</tr></thead>
      <tbody>
      {% for r in rows %}
        <tr>{% for v in r %}<td>{{ v }}</td>{% endfor %}</tr>
      {% endfor %}
      </tbody>
    </table></div>
  {% endif %}
  <script>
    const samples = {{ samples | map(attribute=1) | list | tojson }};
    function setSql(i) { document.querySelector('textarea').value = samples[i]; }
  </script>
</body>
</html>
"""


def _run(sql):
    t0 = time.time()
    cols, rows = [], []
    with psycopg2.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
    ms = round((time.time() - t0) * 1000)
    return cols, rows, ms


@app.route("/", methods=["GET", "POST"])
def index():
    sql = (request.form.get("sql") or DEFAULT_SQL).strip()
    cols, rows, ms, error = [], [], None, None
    if request.method == "POST" and sql:
        try:
            cols, rows, ms = _run(sql)
        except Exception:
            error = traceback.format_exc(limit=1)
    return render_template_string(
        PAGE, sql=sql, samples=SAMPLES, cols=cols, rows=rows, ms=ms, error=error, db=DB
    )


@app.route("/export")
def export():
    sql = (request.args.get("sql") or DEFAULT_SQL).strip()
    try:
        cols, rows, _ = _run(sql)
        import csv
        import io

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        w.writerows(rows)
        body = buf.getvalue()
        from flask import Response

        return Response(
            body,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=query.csv"},
        )
    except Exception as e:
        return str(e), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
