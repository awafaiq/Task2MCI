CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders_master (
    order_id String,
    user_id String,
    order_hour_of_day Int32,
    product_id String,
    product_name String,
    department String,
    add_to_cart_order Int32
) ENGINE = MergeTree()
ORDER BY order_id;