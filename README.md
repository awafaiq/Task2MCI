# Enterprise Data Pipeline: Orders Analytics System

## Deskripsi Proyek

Proyek ini mengimplementasikan arsitektur data pipeline end-to-end yang mengotomatisasi seluruh siklus hidup data pesanan, mulai dari penarikan data mentah hingga visualisasi bisnis. Sistem melakukan ekstraksi data dari endpoint API e-commerce secara periodik, memproses dan membersihkannya melalui Apache Spark, lalu memuatnya ke ClickHouse sebagai data warehouse untuk kebutuhan analitik. Seluruh orkestrasi berjalan di atas Apache Airflow dalam environment Docker Compose yang terisolasi dan reproducible.

---

## Tech Stack

| Komponen | Teknologi | Fungsi |
|---|---|---|
| Orchestration | Apache Airflow 2.9.1 | Penjadwalan dan orkestrasi pipeline setiap 15 menit |
| Data Processing | Apache Spark (PySpark 3.5.1) | Transformasi, validasi, dan pembersihan data skala besar |
| Data Manipulation | Pandas 2.2.1 | Konversi DataFrame untuk loading ke warehouse |
| Data Warehouse | ClickHouse | Columnar database untuk query analitik performa tinggi |
| Business Intelligence | Metabase | Visualisasi data dan pembuatan dashboard interaktif |
| Containerization | Docker Compose | Deployment seluruh service dalam satu environment terisolasi |

---

## Arsitektur dan Alur Kerja (ETL Pipeline)

Berikut adalah alur data dari hulu ke hilir yang dijalankan secara otomatis setiap 15 menit:

```
API Endpoint               Data Lake Lokal           Apache Spark             ClickHouse            Metabase
(e-commerce)               (Parquet Files)           (Processing)             (Warehouse)           (Dashboard)
     |                          |                         |                       |                     |
     |   1. EXTRACT             |                         |                       |                     |
     |   GET /orders  --------->|                         |                       |                     |
     |   Simpan sebagai         |                         |                       |                     |
     |   raw Parquet            |                         |                       |                     |
     |                          |   2. TRANSFORM          |                       |                     |
     |                          |   Baca Parquet -------->|                       |                     |
     |                          |                         |  Explode, Clean,      |                     |
     |                          |                         |  Threshold Filter     |                     |
     |                          |                         |                       |                     |
     |                          |                         |   3. LOAD             |                     |
     |                          |                         |   INSERT INTO ------->|                     |
     |                          |                         |                       |                     |
     |                          |                         |                       |   4. VISUALIZE      |
     |                          |                         |                       |   SQL Query ------->|
     |                          |                         |                       |                     |
```

### Penjelasan Setiap Tahap

**1. Extract -- `fetch_orders_api.py`**

Script ini melakukan HTTP GET request ke endpoint API e-commerce (`http://96.9.212.102:8000/orders`). Data JSON yang diterima langsung dikonversi ke Pandas DataFrame, lalu disimpan sebagai file Parquet di Data Lake lokal (`/opt/airflow/data_lake/orders/`). Setiap file diberi timestamp unik untuk menghindari overwrite.

**2. Transform -- `process_orders_sparks.py`**

Apache Spark membaca seluruh file Parquet dari Data Lake, melakukan explode pada array `products` menjadi baris individual, kemudian melakukan mapping kolom sesuai skema target. Di tahap inilah proses Threshold Filtering dijalankan untuk menjamin kualitas data sebelum masuk ke warehouse.

**3. Load -- ClickHouse Insertion**

Data bersih hasil Spark dikonversi ke Pandas DataFrame, lalu dimuat ke tabel `analytics.orders_master` di ClickHouse menggunakan `clickhouse-driver`. Setelah loading berhasil, file Parquet lama di Data Lake dibersihkan secara otomatis untuk menghemat storage.

**4. Visualize -- Metabase Dashboard**

Metabase terhubung langsung ke ClickHouse dan menjalankan query SQL analitik untuk menghasilkan visualisasi real-time berupa chart dan dashboard interaktif.

### Orkestrasi dengan Apache Airflow

Seluruh tahap di atas diorkestrasi oleh DAG `orders_realtime_pipeline` di Apache Airflow dengan konfigurasi berikut:

- **Schedule Interval:** `*/15 * * * *` (setiap 15 menit, pola micro-batching)
- **Max Active Runs:** 1 (mencegah pipeline collision)
- **Retries:** 2 kali dengan delay 2 menit (antisipasi timeout API)
- **Catchup:** Disabled (hanya memproses data terkini)

Dependensi task:

```
fetch_orders_data >> process_and_load_clickhouse
```

---

## Data Transformation dan Thresholding Logic

Pada tahap Spark Processing, pipeline menerapkan Threshold Filtering sebagai mekanisme pembersihan data untuk menjamin Data Veracity. Filter ini diterapkan pada kolom `add_to_cart_order`, yang merepresentasikan urutan suatu produk ditambahkan ke keranjang belanja oleh konsumen.

### Threshold yang Diterapkan

```python
LOWER_BOUND_BASKET = 1
UPPER_BOUND_BASKET = 70

df_clean = df_extracted.filter(
    (F.col("add_to_cart_order") >= LOWER_BOUND_BASKET) &
    (F.col("add_to_cart_order") <= UPPER_BOUND_BASKET)
)
```

### Rasional di Balik Batas Nilai

**Lower Bound -- `add_to_cart_order >= 1`**

Urutan logis masuk keranjang dimulai dari angka 1. Nilai 0 atau negatif mengindikasikan data yang corrupt atau tidak valid secara semantik, sehingga harus dibuang dari dataset.

**Upper Bound -- `add_to_cart_order <= 70`**

Dalam konteks perilaku belanja retail normal, sangat jarang seorang konsumen menambahkan lebih dari 70 item berbeda ke dalam satu keranjang belanja. Transaksi dengan nilai `add_to_cart_order` di atas 70 diklasifikasikan sebagai anomali atau indikasi fraud (misalnya bot scraping, automated checkout abuse), sehingga difilter agar tidak mencemari hasil analitik downstream.

> **Catatan:** Threshold ini bersifat configurable dan dapat disesuaikan sesuai kebutuhan bisnis tanpa mengubah struktur pipeline.

---

## Skema Database (DDL)

Tabel `analytics.orders_master` di ClickHouse dibuat dengan skema berikut:

```sql
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders_master (
    order_id          String,
    user_id           String,
    order_hour_of_day Int32,
    product_id        String,
    product_name      String,
    department        String,
    add_to_cart_order  Int32
) ENGINE = MergeTree()
ORDER BY order_id;
```

### Penjelasan Kolom

| Kolom | Tipe | Deskripsi |
|---|---|---|
| `order_id` | String | Identifier unik untuk setiap pesanan |
| `user_id` | String | Identifier unik untuk setiap pelanggan |
| `order_hour_of_day` | Int32 | Jam saat pesanan dibuat (0-23) |
| `product_id` | String | Identifier unik untuk setiap produk |
| `product_name` | String | Nama produk yang dipesan |
| `department` | String | Kategori departemen produk |
| `add_to_cart_order` | Int32 | Urutan produk ditambahkan ke keranjang |

Engine `MergeTree()` dipilih karena merupakan engine default ClickHouse yang optimal untuk workload analitik dengan volume insert tinggi dan query agregasi yang berat.

---

## Query SQL untuk Metabase Dashboard

Berikut adalah tiga query analitik yang digunakan untuk membangun visualisasi di Metabase:

### Query 1: Tren Pesanan per Jam (Time Series / Line Chart)

```sql
SELECT
    order_hour_of_day AS hour,
    COUNT(DISTINCT order_id) AS total_orders
FROM analytics.orders_master
GROUP BY hour
ORDER BY hour ASC;
```

Menampilkan distribusi volume pesanan berdasarkan jam dalam sehari untuk mengidentifikasi peak hours dan pola belanja konsumen.

### Query 2: Distribusi Departemen (Pie Chart / Bar Chart)

```sql
SELECT
    department,
    COUNT(*) AS total_products_ordered
FROM analytics.orders_master
GROUP BY department
ORDER BY total_products_ordered DESC;
```

Memvisualisasikan proporsi produk yang dipesan dari masing-masing departemen untuk memahami kategori mana yang paling dominan.

### Query 3: Top 10 Produk Terlaris (Horizontal Bar Chart)

```sql
SELECT
    product_name,
    COUNT(*) AS times_ordered
FROM analytics.orders_master
GROUP BY product_name
ORDER BY times_ordered DESC
LIMIT 10;
```

Menampilkan 10 produk dengan frekuensi pemesanan tertinggi sebagai insight untuk keputusan stocking dan promosi.

---

## Struktur Proyek

```
Task2MCI/
|-- dags/
|   |-- orders_pipeline.py              # Definisi DAG Airflow
|   |-- scripts/
|       |-- fetch_orders_api.py          # Script ekstraksi data dari API
|       |-- process_orders_sparks.py     # Script Spark: transform, clean, load
|-- data_lake/
|   |-- orders/                          # Direktori penyimpanan file Parquet (runtime)
|-- sql/
|   |-- clickhouse_ddl.sql               # DDL pembuatan tabel ClickHouse
|   |-- metabase_queries.sql             # Kumpulan query untuk dashboard Metabase
|-- docker-compose.yml                   # Konfigurasi seluruh service (Airflow, ClickHouse, Metabase)
|-- Dockerfile                           # Custom image Airflow dengan Java Runtime dan dependensi Python
|-- requirements.txt                     # Dependensi Python (PySpark, Pandas, ClickHouse Driver, dll.)
|-- README.md                            # Dokumentasi proyek (file ini)
```

---

## Cara Menjalankan

### Prasyarat

- Docker dan Docker Compose terinstal di mesin lokal.
- Minimal 4 GB RAM tersedia untuk kontainer.
- Port 8080, 8123, 9000, dan 3000 tidak digunakan oleh service lain.

### Langkah Deployment

**1. Clone repository dan masuk ke direktori proyek:**

```bash
git clone <repository-url>
cd Task2MCI
```

**2. Bangun dan jalankan seluruh service:**

```bash
docker-compose up --build -d
```

**3. Tunggu proses inisialisasi selesai, lalu akses masing-masing service:**

| Service | URL | Kredensial |
|---|---|---|
| Airflow Web UI | `http://localhost:8080` | admin / admin |
| ClickHouse HTTP | `http://localhost:8123` | admin / rahasia |
| Metabase | `http://localhost:3000` | (setup saat pertama kali akses) |

**4. Aktifkan DAG `orders_realtime_pipeline` di Airflow Web UI.**

Pipeline akan berjalan otomatis setiap 15 menit setelah DAG diaktifkan. Untuk trigger manual, gunakan tombol "Trigger DAG" pada halaman DAG.

**5. Hubungkan Metabase ke ClickHouse:**

Pada setup awal Metabase, tambahkan database connection dengan konfigurasi berikut:

- **Database type:** ClickHouse
- **Host:** `clickhouse-server`
- **Port:** `8123`
- **Database name:** `analytics`
- **Username:** `admin`
- **Password:** `rahasia`

Setelah terhubung, buat pertanyaan baru (New Question) menggunakan query SQL yang tersedia di bagian sebelumnya.

---

## Dokumentasi Visual

### 1. Airflow DAG Graph View

Tampilan Graph View dari Airflow Web UI yang menunjukkan struktur dan dependensi antar task dalam DAG `orders_realtime_pipeline`.

![Airflow DAG Graph](./assets/airflow_dag.png)

### 2. Airflow Task Execution Logs

Log eksekusi yang menunjukkan seluruh task berhasil dijalankan tanpa error, mencakup output dari proses extract, transform, dan load.

![Airflow Success Logs](./assets/airflow_logs.png)

### 3. Verifikasi Data di ClickHouse

Hasil query `SELECT * FROM analytics.orders_master LIMIT 10` yang memverifikasi bahwa data telah berhasil dimuat ke warehouse dengan skema yang benar.

![ClickHouse Data Verification](./assets/clickhouse_data.png)

### 4. Dashboard Metabase

Dashboard lengkap di Metabase yang menampilkan visualisasi tren pesanan per jam, distribusi departemen, dan top 10 produk terlaris.

![Metabase Dashboard](./assets/metabase_dashboard.png)