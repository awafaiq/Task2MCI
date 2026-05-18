# 🚀 Enterprise Data Pipeline: Orders Analytics System

## 📖 Deskripsi Proyek
Proyek ini mengimplementasikan Arsitektur Data modern dengan mengotomatisasi penarikan data dari endpoint API (`http://96.9.212.102:8000/orders`), memprosesnya menggunakan Apache Spark, menyimpannya di ClickHouse (Columnar Database), dan memvisualisasikannya secara *real-time* di Metabase.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow
- **Data Processing:** Apache Spark & Pandas
- **Data Warehouse:** ClickHouse
- **Business Intelligence:** Metabase
- **Containerization:** Docker Compose

## 🧠 Data Transformation & Thresholding Logic
> **Catatan Penting Pemrosesan Data:**
> Pada tahap Spark Processing (`process_orders_spark.py`), kami menerapkan *Threshold Filtering* untuk memastikan **Data Veracity** berdasarkan *business logic* yang jelas:
> - **Lower Bound (`price > 0`)**: Mencegah pesanan dengan harga nol atau negatif masuk ke *revenue dashboard*.
> - **Upper Bound (`price <= 50000`)**: Memfilter transaksi *outlier* berukuran raksasa yang diklasifikasikan sebagai transaksi B2B/Fraud yang memerlukan pemeriksaan manual, sehingga tidak merusak rata-rata *Daily Active Revenue* retail.

## 📸 Dokumentasi Visual (Screenshots)

### 1. Apache Airflow DAG
*Tampilkan Screenshot *Graph View* dari Airflow Web UI (http://localhost:8080)*
`![Airflow DAG](./assets/airflow_dag.png)`

### 2. Eksekusi Task Sukses
*Tampilkan Screenshot log kesuksesan dari Task Instance*
`![Airflow Logs](./assets/airflow_logs.png)`

### 3. ClickHouse Data Verification
*Tampilkan hasil query `SELECT * FROM analytics.orders_master LIMIT 5` dari DBeaver/ClickHouse Client*
`![ClickHouse Data](./assets/clickhouse_data.png)`

### 4. Metabase Dashboard
*Tampilkan Screenshot Dashboard lengkap di Metabase (http://localhost:3000)*
`![Metabase Dashboard](./assets/metabase_dashboard.png)`