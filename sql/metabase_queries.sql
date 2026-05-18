-- Query 1: 
SELECT
    order_hour_of_day AS hour,
    COUNT(DISTINCT order_id) AS total_orders
FROM analytics.orders_master
GROUP BY hour
ORDER BY hour ASC;

-- Query 2:
SELECT
    department,
    COUNT(*) AS total_products_ordered
FROM analytics.orders_master
GROUP BY department
ORDER BY total_products_ordered DESC;

-- Query 3: 
SELECT
    product_name,
    COUNT(*) AS times_ordered
FROM analytics.orders_master
GROUP BY product_name
ORDER BY times_ordered DESC
LIMIT 10;