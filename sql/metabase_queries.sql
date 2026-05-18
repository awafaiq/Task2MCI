-- Query 1: Total Pendapatan Harian (Time Series)
SELECT 
    toDate(order_date) AS date,
    SUM(price) AS total_revenue
FROM analytics.orders_master
WHERE status = 'Completed'
GROUP BY date
ORDER BY date ASC;

-- Query 2: Distribusi Status Pesanan (Pie Chart)
SELECT 
    status,
    COUNT(order_id) as total_orders
FROM analytics.orders_master
GROUP BY status;

-- Query 3: Produk Terlaris berdasarkan Revenue (Bar Chart)
SELECT 
    product_name,
    SUM(price) as total_revenue
FROM analytics.orders_master
WHERE status = 'Completed'
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;