-- DDL untuk membuat tabel ClickHouse secara manual (opsional, karena script Python sudah meng-handle ini, namun wajib disertakan untuk dokumentasi penilaian).

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders_master (
    order_id String,
    customer_id String,
    product_name String,
    price Float32,
    status String,
    order_date DateTime
) ENGINE = MergeTree()
ORDER BY order_date;