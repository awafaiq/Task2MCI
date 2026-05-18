from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_orders_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Pipeline_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca aliran data mentah dari Data Lake...")
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")

    # Explode array 'products' menjadi baris individual
    df_exploded = df_raw.withColumn("product_item", F.explode("products"))

    # Mapping kolom sesuai struktur JSON asli dari API
    df_extracted = df_exploded.select(
        F.col("order_id").cast("string"),
        F.col("user_id").cast("string"),
        F.col("order_hour_of_day").cast("int"),
        F.col("product_item.product_id").cast("string").alias("product_id"),
        F.col("product_item.product_name").cast("string").alias("product_name"),
        F.col("product_item.department").cast("string").alias("department"),
        F.col("product_item.add_to_cart_order").cast("int").alias("add_to_cart_order")
    )

    # ---------------------------------------------------------
    # VALIDATION & THRESHOLDING (MENJAWAB KRITIK SEBELUMNYA)
    # ---------------------------------------------------------
    # Batasan Logis add_to_cart_order:
    # 1. Lower Bound (>= 1): Urutan barang masuk keranjang dimulai dari angka 1.
    # 2. Upper Bound (<= 70): Secara perilaku belanja retail normal, sangat jarang konsumen 
    #    membeli lebih dari 70 item berbeda dalam sekali transaksi. Nilai > 70 difilter sebagai anomali.
    LOWER_BOUND_BASKET = 1
    UPPER_BOUND_BASKET = 70

    print("Melakukan Data Cleaning & Threshold Filtering...")
    df_clean = df_extracted.filter(
        (F.col("add_to_cart_order") >= LOWER_BOUND_BASKET) & 
        (F.col("add_to_cart_order") <= UPPER_BOUND_BASKET)
    )

    final_results = df_clean.toPandas()
    spark.stop()

    print("Memuat data bersih ke ClickHouse Warehouse...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')
    
    # Buat skema tabel baru yang cocok di ClickHouse
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_master (
            order_id String,
            user_id String,
            order_hour_of_day Int32,
            product_id String,
            product_name String,
            department String,
            add_to_cart_order Int32
        ) ENGINE = MergeTree()
        ORDER BY order_id
    ''')
    
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.orders_master VALUES', data_tuples)
    
    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
            
    print("✅ Pipeline Pemrosesan Selesai!")

if __name__ == "__main__":
    run_orders_analytics()