import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders_data():
    print("Membuka koneksi ke API Endpoint Orders...")
    url = "http://96.9.212.102:8000/orders"
    
    headers = {
        "User-Agent": "MCI-DataPipeline/1.0 (Task2) Python-Requests"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Asumsi API mengembalikan list of dictionaries
        df = pd.DataFrame(data['orders'])
        
        # Simpan ke Data Lake lokal dalam format Parquet
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f'/opt/airflow/data_lake/orders/raw_orders_{current_time}.parquet'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False)
        
        print(f"✅ Sukses menyimpan {len(df)} baris data mentah ke {output_path}")
    except Exception as e:
        print(f"❌ Gagal menarik data dari API: {e}")
        raise

if __name__ == "__main__":
    fetch_orders_data()