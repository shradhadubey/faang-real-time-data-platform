-- ============================================================
-- FAANG Data Platform — Analytics Queries
-- ============================================================
-- Ready-to-run Athena SQL for dashboards, ad-hoc analysis,
-- and business intelligence reporting.
-- Database: faang_data_platform_db
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. REVENUE OVERVIEW
-- ────────────────────────────────────────────────────────────

-- Q1: Total daily revenue for the last 30 days
SELECT
    report_date,
    SUM(hourly_revenue)   AS total_revenue,
    SUM(order_count)      AS total_orders,
    SUM(unique_customers) AS unique_customers,
    ROUND(SUM(hourly_revenue) / NULLIF(SUM(order_count), 0), 2) AS avg_order_value
FROM faang_data_platform_db.gold_daily_revenue
WHERE report_year  >= YEAR(CURRENT_DATE - INTERVAL '30' DAY)
  AND report_month >= MONTH(CURRENT_DATE - INTERVAL '30' DAY)
GROUP BY report_date
ORDER BY report_date DESC;


-- Q2: Revenue by category — top categories this month
SELECT
    category,
    SUM(hourly_revenue)                                           AS total_revenue,
    SUM(order_count)                                             AS total_orders,
    ROUND(SUM(hourly_revenue) * 100.0 / SUM(SUM(hourly_revenue)) OVER (), 2) AS revenue_share_pct
FROM faang_data_platform_db.gold_daily_revenue
WHERE report_year  = YEAR(CURRENT_DATE)
  AND report_month = MONTH(CURRENT_DATE)
GROUP BY category
ORDER BY total_revenue DESC;


-- Q3: Hourly revenue heatmap (hour × day-of-week)
SELECT
    event_hour,
    DAY_OF_WEEK(DATE(report_date))  AS day_of_week,
    ROUND(AVG(hourly_revenue), 2)   AS avg_hourly_revenue
FROM faang_data_platform_db.gold_daily_revenue
WHERE report_year  = YEAR(CURRENT_DATE)
  AND report_month = MONTH(CURRENT_DATE)
GROUP BY event_hour, DAY_OF_WEEK(DATE(report_date))
ORDER BY day_of_week, event_hour;


-- ────────────────────────────────────────────────────────────
-- 2. PRODUCT ANALYTICS
-- ────────────────────────────────────────────────────────────

-- Q4: Top 10 products by revenue
SELECT
    product_id,
    product_name,
    category,
    SUM(daily_revenue)   AS total_revenue,
    SUM(purchase_count)  AS total_sales,
    SUM(cart_add_count)  AS total_cart_adds,
    SUM(view_count)      AS total_views,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion_rate
FROM faang_data_platform_db.gold_top_products
WHERE report_year  = YEAR(CURRENT_DATE)
  AND report_month = MONTH(CURRENT_DATE)
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;


-- Q5: Products with declining revenue (WoW comparison)
WITH this_week AS (
    SELECT product_id, product_name, SUM(daily_revenue) AS revenue
    FROM faang_data_platform_db.gold_top_products
    WHERE DATE(report_date) >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY product_id, product_name
),
last_week AS (
    SELECT product_id, SUM(daily_revenue) AS revenue
    FROM faang_data_platform_db.gold_top_products
    WHERE DATE(report_date) >= CURRENT_DATE - INTERVAL '14' DAY
      AND DATE(report_date) <  CURRENT_DATE - INTERVAL '7'  DAY
    GROUP BY product_id
)
SELECT
    t.product_name,
    ROUND(t.revenue, 2)  AS this_week_revenue,
    ROUND(l.revenue, 2)  AS last_week_revenue,
    ROUND((t.revenue - l.revenue) / NULLIF(l.revenue, 0) * 100, 2) AS wow_change_pct
FROM this_week t
JOIN last_week l USING (product_id)
WHERE t.revenue < l.revenue
ORDER BY wow_change_pct ASC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 3. USER & FUNNEL ANALYTICS
-- ────────────────────────────────────────────────────────────

-- Q6: Purchase funnel — conversion rates across event types
SELECT
    event_type,
    COUNT(DISTINCT user_id)  AS unique_users,
    COUNT(*)                 AS event_count,
    ROUND(COUNT(DISTINCT user_id) * 100.0
          / MAX(COUNT(DISTINCT user_id)) OVER (), 2) AS funnel_pct
FROM faang_data_platform_db.silver_events
WHERE event_year  = YEAR(CURRENT_DATE)
  AND event_month = MONTH(CURRENT_DATE)
  AND event_day   = DAY(CURRENT_DATE - INTERVAL '1' DAY)
GROUP BY event_type
ORDER BY unique_users DESC;


-- Q7: Active users by device type and country
SELECT
    country,
    device_type,
    COUNT(DISTINCT user_id)  AS daily_active_users,
    COUNT(*)                 AS total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) AS revenue
FROM faang_data_platform_db.silver_events
WHERE event_year  = YEAR(CURRENT_DATE)
  AND event_month = MONTH(CURRENT_DATE)
  AND event_day   = DAY(CURRENT_DATE - INTERVAL '1' DAY)
GROUP BY country, device_type
ORDER BY daily_active_users DESC
LIMIT 50;


-- Q8: High-value customers (top 1%)
SELECT
    user_id,
    COUNT(DISTINCT event_date)       AS active_days,
    COUNT(*)                         AS total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS orders,
    ROUND(SUM(COALESCE(revenue, 0)), 2) AS total_spend
FROM faang_data_platform_db.silver_events
WHERE event_year  = YEAR(CURRENT_DATE)
  AND event_month = MONTH(CURRENT_DATE)
GROUP BY user_id
HAVING SUM(COALESCE(revenue, 0)) > 500
ORDER BY total_spend DESC
LIMIT 100;


-- ────────────────────────────────────────────────────────────
-- 4. REAL-TIME MONITORING (last 2 hours)
-- ────────────────────────────────────────────────────────────

-- Q9: Revenue and order volume in last 2 hours (streaming Gold)
SELECT
    window_start,
    window_end,
    SUM(total_revenue)    AS revenue,
    SUM(total_orders)     AS orders,
    SUM(unique_buyers)    AS unique_buyers
FROM faang_data_platform_db.gold_sales_metrics
WHERE window_start >= NOW() - INTERVAL '2' HOUR
GROUP BY window_start, window_end
ORDER BY window_start DESC;


-- Q10: Geographic revenue map (last 24h)
SELECT
    country,
    SUM(total_revenue) AS revenue,
    SUM(total_orders)  AS orders
FROM faang_data_platform_db.gold_sales_metrics
WHERE window_start >= NOW() - INTERVAL '24' HOUR
GROUP BY country
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 5. DATA QUALITY MONITORING
-- ────────────────────────────────────────────────────────────

-- Q11: Daily record counts and null rates (DQ trending)
SELECT
    CONCAT(CAST(report_year AS VARCHAR), '-',
           LPAD(CAST(report_month AS VARCHAR), 2, '0'), '-',
           LPAD(CAST(report_day AS VARCHAR),   2, '0')) AS report_date,
    total_records,
    null_event_id_pct,
    null_user_id_pct,
    duplicate_event_ids,
    status
FROM faang_data_platform_db.gold_dq_reports
ORDER BY report_year DESC, report_month DESC, report_day DESC
LIMIT 30;
