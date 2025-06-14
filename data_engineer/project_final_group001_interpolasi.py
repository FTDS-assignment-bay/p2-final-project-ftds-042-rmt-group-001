# Import libraries
import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.python import PythonOperator 
import pandas as pd
import psycopg2 as db


# fungsi untuk mengambil data dari PostgreSQL dan menyimpan ke CSV 
def get_data_from_db():
    # Membuat string koneksi ke database PostgreSQL
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    # Membuka koneksi ke database
    conn = db.connect(conn_string)
    # Menjalankan query SQL untuk mengambil data dari provinsi-provinsi Pulau Jawa saja
    df = pd.read_sql("""SELECT * FROM table_jawa WHERE provinsi ILIKE ANY (ARRAY[
    '%Banten%',
    '%DKI%',
    '%Jawa Barat%',
    '%Jawa Tengah%',
    '%Jawa Timur%',
    '%Yogyakarta%'
    ]);""", conn)
    # Menyimpan DataFrame hasil query ke file CSV tanpa menyertakan index
    df.to_csv('/opt/airflow/dags/dataset_scrapping.csv', index=False)


# fungsi untuk pembersihan data
def data_preprocessing():
    # Loading CSV ke DataFrame
    df_data = pd.read_csv('/opt/airflow/dags/dataset_scrapping.csv') 

    # Mengonversi kolom 'date' menjadi tipe datetime
    df_data['date'] = pd.to_datetime(df_data['date'])

    # Menjadikan kolom 'date' sebagai index untuk keperluan interpolasi berbasis waktu
    df_data.set_index('date', inplace=True)

    # handle missing value dengan Interpolasi. Dilakukan per kombinasi komoditas dan provinsi
    df_data['harga'] = df_data.groupby(by=['komoditas', 'provinsi'])['harga'].transform(lambda x: x.interpolate(method='time', limit_area='inside'))
    df_data.reset_index(inplace=True)

    # Menghapus baris yang masih mengandung nilai kosong (jika ada yang tidak terisi oleh interpolasi)
    df_data.dropna(inplace=True)

    # Menyimpan DataFrame yang sudah dibersihkan ke file CSV baru tanpa menyertakan index
    df_data.to_csv('/opt/airflow/dags/data_clean_interpolasi.csv', index=False)


# fungsi untuk post data ke Elasticsearch
def post_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/data_clean_interpolasi.csv')

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_jawa_interpolasi", id=i + 1, body=doc)


# DAG setup
default_args = {
    'owner': 'cana', # nama pemilik DAG
    'depends_on_past': False, # task tidak bergantung pada keberhasilan task run sebelumnya
    'email_on_failure': False, # tidak kirim email jika task gagal
    'email_on_retry': False, # tidak kirim email saat task di-retry
    'retries': 1, # jumlah maksimal percobaan ulang jika task gagal
    'retry_delay': timedelta(minutes=1), # jeda 1 menit sebelum mencoba ulang
}

with DAG('project_final_group001_interpolasi',
         description='final_projek_group_001_interpolasi',
         default_args=default_args,
         schedule_interval='0 0 * * *',
         start_date=dt.datetime(2025, 6, 13) + timedelta(hours=7), # (UTC -7) karena saya berada di zona pdt
         catchup=False) as dag:

    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_db', 
        python_callable=get_data_from_db
    )

    # Task that will be executed by PythonOperator
    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_preprocessing
    )

    # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    # Set task dependencies
    fetch_task >> clean_task >> post_to_kibana_task