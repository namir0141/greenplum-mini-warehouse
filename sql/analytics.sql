-- ─────────────────────────────────────────────────────────────────────
-- Level 3 — Analytical queries over the gold star schema (Greenplum).
-- Run from the project root, e.g.:
--   psql -c "SELECT * FROM gold.dim_date LIMIT 5"
--
-- Placeholders (<gold_fact_orders> etc.) are expanded by src/analytics.py
-- to schema-qualified table names.
-- ─────────────────────────────────────────────────────────────────────

-- 1. Revenue by month ------------------------------------------------
SELECT d.year, d.month, d.month_name, ROUND(SUM(f.revenue)::numeric, 2) AS revenue
FROM <gold_fact_orders> f
JOIN <gold_dim_date> d ON f.date_key = d.date_key
WHERE f.status != 'cancelled'
GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- 2. Customer lifetime value (top 10) --------------------------------
SELECT customer_key, COUNT(*) AS orders,
       ROUND(SUM(revenue)::numeric, 2) AS clv,
       ROUND((SUM(revenue) / COUNT(*))::numeric, 2) AS avg_order_value
FROM <gold_fact_orders>
WHERE status != 'cancelled'
GROUP BY 1
ORDER BY clv DESC
LIMIT 10;

-- 3. Repeat customer rate ---------------------------------------------
WITH freq AS (
    SELECT customer_key, COUNT(*) AS n
    FROM <gold_fact_orders>
    WHERE status != 'cancelled'
    GROUP BY 1
)
SELECT COUNT(*) FILTER (WHERE n >= 2) AS repeat_customers,
       COUNT(*) AS total_customers,
       ROUND((100.0 * COUNT(*) FILTER (WHERE n >= 2) / NULLIF(COUNT(*), 0))::numeric, 2) AS repeat_rate_pct
FROM freq;

-- 4. Average order value ----------------------------------------------
SELECT ROUND(AVG(revenue)::numeric, 2) AS avg_order_value,
       COUNT(*) AS orders,
       ROUND(SUM(revenue)::numeric, 2) AS total_revenue
FROM <gold_fact_orders>
WHERE status NOT IN ('cancelled', 'refunded');

-- 5. Product profitability by category --------------------------------
SELECT p.category,
       ROUND(SUM(i.amount)::numeric, 2) AS revenue,
       ROUND(SUM(i.quantity * p.cost)::numeric, 2) AS cost,
       ROUND((100.0 * (SUM(i.amount) - SUM(i.quantity * p.cost))
              / NULLIF(SUM(i.amount), 0))::numeric, 1) AS margin_pct
FROM <gold_fact_order_items> i
JOIN <gold_dim_product> p ON i.product_key = p.product_key
GROUP BY 1
ORDER BY margin_pct DESC;

-- 6. Cohort retention (monthly cohorts, month-offset view) -------------
WITH cohorts AS (
    SELECT customer_key, date_trunc('month', MIN(full_date)) AS cohort_month
    FROM <gold_fact_orders> f
    JOIN <gold_dim_date> d USING (date_key)
    WHERE status != 'cancelled'
    GROUP BY 1
),
activity AS (
    SELECT customer_key, date_trunc('month', full_date) AS month
    FROM <gold_fact_orders> f
    JOIN <gold_dim_date> d USING (date_key)
    WHERE status != 'cancelled'
    GROUP BY 1, 2
)
SELECT CAST(c.cohort_month AS DATE) AS cohort_month,
       (EXTRACT(YEAR FROM a.month) - EXTRACT(YEAR FROM c.cohort_month)) * 12
         + EXTRACT(MONTH FROM a.month) - EXTRACT(MONTH FROM c.cohort_month) AS month_offset,
       COUNT(DISTINCT a.customer_key) AS customers
FROM cohorts c
JOIN activity a USING (customer_key)
WHERE a.month >= c.cohort_month
GROUP BY 1, 2
ORDER BY 1, 2;

-- 7. Customer segmentation (RFM-style) --------------------------------
WITH rfm AS (
    SELECT customer_key,
           (CURRENT_DATE - MAX(full_date))::INT AS recency_days,
           COUNT(*) AS frequency,
           SUM(revenue) AS monetary
    FROM <gold_fact_orders> f
    JOIN <gold_dim_date> d USING (date_key)
    WHERE status != 'cancelled'
    GROUP BY 1
)
SELECT segment, COUNT(*) AS customers, ROUND(SUM(monetary)::numeric, 0) AS total_revenue
FROM (
    SELECT customer_key, monetary,
           CASE
               WHEN recency_days <= 30  AND frequency >= 10 AND monetary >= 1000 THEN 'champions'
               WHEN recency_days <= 90  AND frequency >= 5  THEN 'loyal'
               WHEN recency_days <= 180 AND frequency >= 2  THEN 'active'
               WHEN frequency >= 3 THEN 'at_risk'
               ELSE 'dormant'
           END AS segment
    FROM rfm
) s
GROUP BY 1
ORDER BY customers DESC;

-- 8. Revenue by geography ----------------------------------------------
SELECT c.region, c.country, ROUND(SUM(f.revenue)::numeric, 2) AS revenue
FROM <gold_fact_orders> f
JOIN <gold_dim_customer> c ON f.customer_key = c.customer_key
WHERE f.status != 'cancelled'
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 15;

-- 9. The requirements' example experiment ------------------------------
SELECT customer_id, ROUND(SUM(total_amount)::numeric, 2) AS revenue
FROM <bronze_orders_flat>
WHERE order_date >= DATE '2026-01-01'
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10;
