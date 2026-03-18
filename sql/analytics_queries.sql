-- ============================================================
-- FAANG Data Platform — Analytics Queries
-- ============================================================
-- All queries run against faang_data_platform_db in Athena
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. TOP PRODUCTS
-- ────────────────────────────────────────────────────────────

-- Q1: Top 10 products by revenue
SELECT
    revenue_rank,
    product_name,
    category,
    ROUND(revenue, 2)                   AS revenue,
    purchase_count,
    cart_adds,
    product_views,
    ROUND(conversion_rate * 100, 2)     AS conversion_pct,
    unique_users
FROM gold_top_products
ORDER BY revenue_rank ASC
LIMIT 10;


-- Q2: Best converting products (high cart-to-purchase rate)
SELECT
    product_name,
    category,
    cart_adds,
    purchase_count,
    ROUND(conversion_rate * 100, 2)     AS conversion_pct,
    ROUND(revenue, 2)                   AS revenue
FROM gold_top_products
WHERE cart_adds > 0
ORDER BY conversion_rate DESC
LIMIT 10;


-- Q3: Products with most views but low conversion (optimization targets)
SELECT
    product_name,
    product_views,
    purchase_count,
    ROUND(conversion_rate * 100, 2)     AS conversion_pct,
    ROUND(revenue, 2)                   AS revenue
FROM gold_top_products
WHERE product_views > 10
  AND conversion_rate < 0.1
ORDER BY product_views DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 2. REVENUE ANALYTICS
-- ────────────────────────────────────────────────────────────

-- Q4: Total revenue summary
SELECT
    SUM(total_revenue)                  AS total_revenue,
    SUM(total_orders)                   AS total_orders,
    SUM(unique_buyers)                  AS unique_buyers,
    ROUND(SUM(total_revenue) /
          NULLIF(SUM(total_orders), 0), 2) AS avg_order_value
FROM gold_revenue_metrics;


-- Q5: Revenue by country (geographic breakdown)
SELECT
    country,
    ROUND(SUM(total_revenue), 2)        AS total_revenue,
    SUM(total_orders)                   AS total_orders,
    SUM(unique_buyers)                  AS unique_buyers,
    ROUND(SUM(total_revenue) * 100.0 /
          SUM(SUM(total_revenue)) OVER(), 2) AS revenue_share_pct
FROM gold_revenue_metrics
GROUP BY country
ORDER BY total_revenue DESC;


-- Q6: Revenue by product category
SELECT
    category,
    ROUND(SUM(total_revenue), 2)        AS total_revenue,
    SUM(total_orders)                   AS total_orders,
    ROUND(AVG(avg_order_value), 2)      AS avg_order_value
FROM gold_revenue_metrics
WHERE category IS NOT NULL
GROUP BY category
ORDER BY total_revenue DESC;


-- Q7: Hourly revenue heatmap (best hours of the day)
SELECT
    event_hour,
    ROUND(SUM(total_revenue), 2)        AS revenue,
    SUM(total_orders)                   AS orders,
    SUM(unique_buyers)                  AS buyers
FROM gold_revenue_metrics
GROUP BY event_hour
ORDER BY event_hour ASC;


-- Q8: Revenue by country AND category (cross-tab)
SELECT
    country,
    category,
    ROUND(SUM(total_revenue), 2)        AS revenue,
    SUM(total_orders)                   AS orders
FROM gold_revenue_metrics
WHERE category IS NOT NULL
GROUP BY country, category
ORDER BY revenue DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 3. USER ANALYTICS
-- ────────────────────────────────────────────────────────────

-- Q9: User segment distribution
SELECT
    user_segment,
    COUNT(*)                            AS user_count,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER(), 2)      AS pct_of_users,
    ROUND(AVG(daily_spend), 2)          AS avg_spend,
    SUM(order_count)                    AS total_orders,
    ROUND(AVG(event_count), 1)          AS avg_events
FROM gold_user_segments
GROUP BY user_segment
ORDER BY avg_spend DESC;


-- Q10: Users by device type
SELECT
    device_type,
    COUNT(DISTINCT user_id)             AS users,
    ROUND(SUM(daily_spend), 2)          AS total_spend,
    ROUND(AVG(daily_spend), 2)          AS avg_spend,
    SUM(order_count)                    AS orders
FROM gold_user_segments
GROUP BY device_type
ORDER BY total_spend DESC;


-- Q11: High value users (VIP + high_value segments)
SELECT
    user_id,
    country,
    device_type,
    user_segment,
    ROUND(daily_spend, 2)               AS spend,
    order_count,
    event_count
FROM gold_user_segments
WHERE user_segment IN ('vip', 'high_value')
ORDER BY daily_spend DESC
LIMIT 20;


-- Q12: Users by country
SELECT
    country,
    COUNT(DISTINCT user_id)             AS users,
    ROUND(SUM(daily_spend), 2)          AS total_spend,
    COUNT(CASE WHEN user_segment = 'buyer'
               OR user_segment = 'high_value'
               OR user_segment = 'vip'
               THEN 1 END)              AS buyers
FROM gold_user_segments
GROUP BY country
ORDER BY total_spend DESC;


-- ────────────────────────────────────────────────────────────
-- 4. SILVER LAYER — EVENT ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q13: Event type distribution (funnel view)
SELECT
    event_type,
    COUNT(*)                            AS event_count,
    COUNT(DISTINCT user_id)             AS unique_users,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER(), 2)      AS pct_of_events
FROM silver_events
GROUP BY event_type
ORDER BY event_count DESC;


-- Q14: Revenue per category from Silver
SELECT
    category,
    COUNT(*)                            AS purchase_events,
    ROUND(SUM(revenue), 2)              AS total_revenue,
    ROUND(AVG(price), 2)                AS avg_price,
    SUM(quantity)                       AS units_sold
FROM silver_events
WHERE event_type = 'purchase'
  AND category IS NOT NULL
GROUP BY category
ORDER BY total_revenue DESC;


-- Q15: Data quality check — null rates
SELECT
    COUNT(*)                            AS total_records,
    SUM(CASE WHEN event_id IS NULL
             THEN 1 ELSE 0 END)         AS null_event_ids,
    SUM(CASE WHEN user_id IS NULL
             THEN 1 ELSE 0 END)         AS null_user_ids,
    SUM(CASE WHEN event_type IS NULL
             THEN 1 ELSE 0 END)         AS null_event_types,
    SUM(CASE WHEN event_type = 'purchase'
              AND revenue IS NULL
             THEN 1 ELSE 0 END)         AS purchases_missing_revenue
FROM silver_events;
