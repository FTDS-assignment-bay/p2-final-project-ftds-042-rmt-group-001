# Import libraries
import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.python import PythonOperator 
import pandas as pd
import psycopg2 as db
from dateutil.relativedelta import relativedelta 
import requests
import traceback
import os
import numpy as np
# from fake_useragent import UserAgent

# Menghasilkan path file CSV berdasarkan ID komoditas dan rentang tanggal
def get_filename_csv(komoditas_id, start_date, end_date): 
    return f'/opt/airflow/dags/badanpangan/{komoditas_id}/badanpangangoid_{komoditas_id}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'

# Mendapatkan hari terakhir dari bulan yang diberikan
def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)

# Menghasilkan list periode bulanan dari start_date sampai end_date
def get_monthly_periods(start_date, end_date, ends_today = True):
    current_date = start_date

    periods = []

    while current_date < end_date:
        current_last_day = last_day_of_month(current_date)
        if current_last_day > end_date and ends_today:
            current_last_day = end_date
        # period_date = f'{current_date.strftime("%d/%m/%Y")}%20-%20{current_last_day.strftime("%d/%m/%Y")}'
        periods.append((current_date, current_last_day))
        next_month = current_date + relativedelta(months=1)
        current_date = next_month

    return periods

# Mengecek file yang sudah ada di direktori berdasarkan struktur folder komoditas_id
def get_existing_periods(directory_path = '/opt/airflow/dags/badanpangan'):
    # Get all entries (files and directories)
    komoditas_ids = os.listdir(directory_path)
    result = {}
    for komoditas_id in komoditas_ids:
        if not komoditas_id.isnumeric():
            continue
        all_entries = os.listdir(f'{directory_path}/{komoditas_id}')
        filenames = [x.replace('.csv', '').split('_') for x in all_entries]
        for f in filenames:
            start_date = datetime(int(f[2][:4]), int(f[2][4:6]), int(f[2][6:]))
            end_date = datetime(int(f[3][:4]), int(f[3][4:6]), int(f[3][6:]))
            if komoditas_id not in result:
                result[komoditas_id] = []
            result[komoditas_id].append((start_date, end_date))
    return result

# Mengekstrak JSON hasil response dari API badan pangan menjadi dataframe
def extract_badanpangan_json(response, komoditas):
    results = []
    for data in response['data']:
        for bydate in data['by_date']:
            # df.loc[-1] = [2, 3, 4]
            # komoditas_dfs[komoditas].append({'date'})
            # komoditas_dfs[komoditas].iloc[-1] = [bydate['date'], data['name'], bydate['rata_rata']]
            results.append({
                'komoditas': komoditas,
                'date': bydate['date'],
                'provinsi': data['name'],
                'harga': bydate['rata_rata']
            })
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'].str.replace('-', '/'), format='%d/%m/%Y')
    return df

# Dictionary ID dan nama komoditas utama yang digunakan
komoditas_ids_arr_all = {
    "28": "Beras Medium",
    "35": "Daging Ayam Ras",
    "36": "Telur Ayam Ras",
    "38": "Minyak Goreng Kemasan",
    # "152": "Daging Kerbau Segar (Lokal)",
    # "149": "Daging Kerbau Beku (Impor Luar Negeri)",
    # "127": "Minyakita",
    # "109": "Beras SPHP",
    # "108": "Tepung Terigu Kemasan",
    # "106": "Ikan Bandeng",
    # "105": "Ikan Tongkol",
    # "104": "Ikan Kembung",
    # "102": "Jagung Tk Peternak",
    # "101": "Minyak Goreng Curah",
    # "29": "Kedelai Biji Kering (Impor)",
    # "40": "Tepung Terigu (Curah)",
    # "27": "Beras Premium",
    # "107": "Garam Konsumsi",
    # "126": "Cabai Merah Besar",
    # "32": "Cabai Merah Keriting",
    # "33": "Cabai Rawit Merah",
    # "34": "Daging Sapi Murni",
    # "31": "Bawang Putih Bonggol",
    # "30": "Bawang Merah",
    # "37": "Gula Konsumsi",
}

# Dictionary ID dan nama seluruh provinsi di Indonesia
provinsi_ids_arr_all = {
    "1": "Aceh",
    "2": "Sumatera Utara",
    "3": "Sumatera Barat",
    "4": "Riau",
    "5": "Jambi",
    "6": "Sumatera Selatan",
    "7": "Bengkulu",
    "8": "Lampung",
    "9": "Kepulauan Bangka Belitung",
    "10": "Kepulauan Riau",
    "11": "DKI Jakarta",
    "12": "Jawa Barat",
    "13": "Jawa Tengah",
    "14": "D.I Yogyakarta",
    "15": "Jawa Timur",
    "16": "Banten",
    "17": "Bali",
    "18": "Nusa Tenggara Barat",
    "19": "Nusa Tenggara Timur",
    "20": "Kalimantan Barat",
    "21": "Kalimantan Tengah",
    "22": "Kalimantan Selatan",
    "23": "Kalimantan Timur",
    "24": "Kalimantan Utara",
    "25": "Sulawesi Utara",
    "26": "Sulawesi Tengah",
    "27": "Sulawesi Selatan",
    "28": "Sulawesi Tenggara",
    "29": "Gorontalo",
    "30": "Sulawesi Barat",
    "31": "Maluku",
    "32": "Maluku Utara",
    "33": "Papua Barat",
    "34": "Papua",
    "35": "Papua Barat Daya",
    "36": "Papua Pegunungan",
    "37": "Papua Tengah",
    "38": "Papua Selatan",
    # "": "Nasional"
}

# fungsi untuk scrapping data dari website badanpangan.go.id
def scrap_from_badanpangan_api():
    start_date = datetime(2021, 1, 1)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    komoditas_ids_arr = np.array(list(komoditas_ids_arr_all.keys()))
    np.random.shuffle(komoditas_ids_arr)
    komoditas_ids_arr = list(komoditas_ids_arr)

    periods = get_monthly_periods(start_date, end_date)
    # Only latest month
    periods = [periods[-1]]

    existing_periods = get_existing_periods()

    existing_start_periods = {k: [x[0] for x in v] for k, v in existing_periods.items()}

    url = 'https://api-panelhargav2.badanpangan.go.id/api/front/table-rekapitulasi-komoditas?period_date={period_date}&level_harga_id=3&province_id=&komoditas_id={komoditas_id}'

    results = {}

    max_retries = 5

    # ua = UserAgent()
    for komoditas_id in komoditas_ids_arr:
        komoditas = komoditas_ids_arr_all[komoditas_id]
        results[komoditas_id] = []
        try:
            os.makedirs(f'/opt/airflow/dags/badanpangan/{komoditas_id}', exist_ok=True)
            for period_date_tuple in periods:
                period_date = f'{period_date_tuple[0].strftime("%d/%m/%Y")} - {period_date_tuple[1].strftime("%d/%m/%Y")}'
                current_url = url.format(period_date = period_date.replace(' ', '%20'), komoditas_id = komoditas_id)
                # print(current_url)
                replace_csv = False
                if komoditas_id in existing_periods:
                    if period_date_tuple in existing_periods[komoditas_id]:
                        print(f'SKIP komoditas[{komoditas}] period[{period_date}] - CSV already exists')
                        continue
                    elif period_date_tuple[0] in existing_start_periods[komoditas_id]:
                        replace_csv = existing_start_periods[komoditas_id].index(period_date_tuple[0])
                        if period_date_tuple[1] > existing_periods[komoditas_id][replace_csv][1]:
                            print(f'REPLACE komoditas[{komoditas}] period[{period_date}] :', current_url)
                        else:
                            replace_csv = False
                if replace_csv == False:
                    print(f'GET komoditas[{komoditas}] period[{period_date}] :', current_url)
                
                retry = 0
                response = False
                # header = {'User-Agent':str(ua.random)}
                header = {}
                while retry < max_retries + 1:
                    try:
                        response = requests.get(current_url, headers=header)
                        df_result = extract_badanpangan_json(response.json(), komoditas)
                        break
                    except requests.exceptions.Timeout as e:
                        print(f'  FAILED TIMEOUT [{e.errno}] - RETRY #{retry}')
                        retry += 1
                    except json.decoder.JSONDecodeError as e:
                        print(f'  FAILED JSON [{e.msg}] - RETRY #{retry}')
                        print('  ', response)
                        if response.status_code == 429:
                            # header = {'User-Agent':str(ua.random)}
                            time.sleep(1)
                        retry += 1
                
                if retry > max_retries:
                    print(f'  FETCH FAILED IN {retry-1} RETRIES - GET NEXT PERIOD')
                    continue
                
                
                if replace_csv != False:
                    os.remove(get_filename_csv(komoditas_id, period_date_tuple[0], existing_periods[komoditas_id][replace_csv][1]))
                    print(f'  DELETE_CSV_PREV komoditas[{komoditas}] period[{period_date_tuple[0].strftime("%d/%m/%Y")} - {existing_periods[komoditas_id][replace_csv][1].strftime("%d/%m/%Y")}]')
                    
                tmp_filename = get_filename_csv(komoditas_id, period_date_tuple[0], period_date_tuple[1])
                df_result.to_csv(tmp_filename, index=False)
                print(f'  SAVE_CSV komoditas[{komoditas}] period[{period_date}] size[{df_result.shape}]:', tmp_filename)
        except Exception as e:
            print(traceback.format_exc())
            continue

def from_scrap_to_db():
    # Ambil data periode terakhir dari setiap komoditas yang sudah ada di direktori
    periods = get_existing_periods()
    # Only latest month
    # print(periods)
    # periods = [periods[-1]]
    periods = {k: [v[-1]] for k, v in periods.items() if k in komoditas_ids_arr_all}
    
    insert_query_fill = []
    # Loop untuk setiap komoditas dan periode terakhirnya
    for komoditas_id, period in periods.items():
        for p in period:
            df = pd.read_csv(get_filename_csv(komoditas_id, p[0], p[1]))
            print(df)
            
            # Ambil kolom penting dan duplikasikan untuk kondisi pengecekan
            df2 = pd.DataFrame()
            df2['date'] = df['date']
            df2['komoditas'] = df['komoditas']
            df2['provinsi'] = df['provinsi']
            df2['harga'] = df['harga']

            df2['date_where'] = df2['date']
            df2['komoditas_where'] = df2['komoditas']
            df2['provinsi_where'] = df2['provinsi']
            
            insert_query_fill = insert_query_fill + df2.values.tolist()
    # print(insert_query_fill)
    # return
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    cursor = conn.cursor()
    insert_query = """INSERT INTO table_jawa
    (date, komoditas, provinsi, harga)
    SELECT %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT date, komoditas, provinsi FROM table_jawa 
        WHERE date = %s
        AND komoditas = %s
        AND provinsi = %s  
    );"""
    # Executing the batch INSERT statement
    cursor.executemany(insert_query, insert_query_fill)

    # Committing the transaction
    conn.commit()
    conn.close()

# fungsi untuk mengambil data dari PostgreSQL dan menyimpan ke CSV 
def get_data_from_db():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("""SELECT * FROM table_jawa WHERE provinsi ILIKE ANY (ARRAY[
    '%Banten%',
    '%DKI%',
    '%Jawa Barat%',
    '%Jawa Tengah%',
    '%Jawa Timur%',
    '%Yogyakarta%'
    ]);""", conn)
    df.to_csv('/opt/airflow/dags/dataset_scrapping.csv', index=False)

# fungsi untuk pembersihan data
def data_preprocessing():
    # Loading CSV ke DataFrame
    df_data = pd.read_csv('/opt/airflow/dags/dataset_scrapping.csv') 

    # Menghapus baris yang mengandung missing value
    df_data['date'] = pd.to_datetime(df_data['date'])
    df_data.set_index('date', inplace=True)
    df_data['harga'] = df_data.groupby(by=['komoditas', 'provinsi'])['harga'].transform(lambda x: x.interpolate(method='time', limit_area='inside'))
    df_data.reset_index(inplace=True)
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

with DAG('final_project_group01_rmt042',
         description='final_project_group001_rmt042',
         default_args=default_args,
         schedule_interval='0 0 * * *', # menjadwalkan setiap Sabtu pukul 09:10, 09:20, dan 09:30
         start_date=dt.datetime(2025, 6, 13) + timedelta(hours=7), # (UTC -7) karena saya berada di zona pdt
         catchup=False) as dag: 
    # Task to fetch data from badan pangan API
    scrapping_task = PythonOperator(
        task_id='scrap_from_badanpangan_api', 
        python_callable=scrap_from_badanpangan_api
    )
    # Load to PostgreSQL
    save_database_task = PythonOperator(
        task_id='from_scrap_to_db', 
        python_callable=from_scrap_to_db
    )
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
    scrapping_task >> save_database_task >> fetch_task >> clean_task >> post_to_kibana_task