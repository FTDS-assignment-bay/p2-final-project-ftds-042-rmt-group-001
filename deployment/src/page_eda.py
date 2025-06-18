import streamlit as st
import pandas as pd
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt

def tren_per_month(data,tahun):
    # data_2023 = data[data['date'].dt.year == tahun].copy()

    # Tambahkan kolom bulan
    data['month'] = data['date'].dt.month
    avg_per_month_komoditas = data.groupby(['month', 'komoditas'])['harga'].mean().reset_index()

    fig = plt.figure(figsize=(8, 6))
    sns.lineplot(data=avg_per_month_komoditas, x='month', y='harga', hue='komoditas', marker='o')

    plt.title(f'Trend Rata-rata Harga per Komoditas per Bulan {tahun}')
    plt.xlabel('Tahun')
    plt.ylabel('Harga (Rata-rata)')
    plt.legend(title='Komoditas')
    plt.grid(True)
    plt.tight_layout()
    st.pyplot(fig)

def insight_per_month(tahun):
    if tahun == 2021:
        st.markdown("""
            ## 📅 Insight Tren Harga Komoditas (Per Bulan)

            ### 1. Daging Ayam Ras
            - Terjadi **kenaikan signifikan pada April–Mei**, dengan **puncak harga di Mei sekitar Rp 38.000**.
            - Setelah itu, harga **turun drastis hingga Juli**, lalu **relatif stabil** di sisa tahun.
            - 🔍 Pola ini mencerminkan **lonjakan konsumsi menjelang Idul Fitri (Mei)** dan **penurunan pasca Lebaran**.

            ---

            ### 2. Telur Ayam Ras
            - Harga cenderung **stabil di kisaran Rp 23.000–Rp 24.000** hingga bulan Juli.
            - Terjadi **penurunan tajam pada Agustus–Oktober** (turun hingga sekitar **Rp 19.000**), lalu **rebound pada Desember**.
            - 🔍 Penurunan ini bisa disebabkan oleh **surplus pasokan** atau **penurunan permintaan pasca libur besar**.

            ---

            ### 3. Minyak Goreng Kemasan
            - Harga **relatif stabil di bawah Rp 15.000** hingga bulan September.
            - Terjadi **lonjakan drastis pada Oktober–Desember**, hingga menyentuh hampir **Rp 19.000** di akhir tahun.
            - 🔍 Kenaikan ini kemungkinan dipicu oleh **krisis minyak goreng nasional/global tahun 2021**, terutama akibat **kenaikan harga CPO (minyak sawit mentah)**.

            ---

            ### 4. Beras Medium
            - Merupakan komoditas dengan **harga paling stabil sepanjang tahun**, berkisar antara **Rp 10.000–Rp 10.500**.
            - Hampir **tidak menunjukkan perubahan berarti**.
            - 🔍 Stabilitas ini mencerminkan **peran pemerintah dalam menjaga harga beras** melalui **cadangan beras pemerintah dan subsidi**.

            """)
    elif tahun == 2022:
        st.markdown("""
            ## 📊 Insight Tren Harga Komoditas Sepanjang Tahun

            ### 1. Daging Ayam Ras
            - Harga **cukup tinggi dan fluktuatif sepanjang tahun**.
            - Terjadi **lonjakan tertinggi pada bulan Juli**, mencapai puncak sekitar **Rp 37.500**.
            - 🔍 Kenaikan ini bisa disebabkan oleh:
            - **Kenaikan biaya produksi** (pakan, DOC).
            - **Momen hari besar keagamaan** seperti **Idul Adha**.
            - **Ketidakseimbangan pasokan dan permintaan**.

            ---

            ### 2. Telur Ayam Ras
            - Harga **naik konsisten dari Maret ke Juli**, dengan puncak tertinggi di **bulan Agustus**.
            - Setelah itu, harga **sedikit menurun**, lalu **naik kembali di Desember**.
            - 🔍 Pola ini menunjukkan permintaan meningkat pada semester kedua, kemungkinan karena:
            - **Kebutuhan rumah tangga dan industri makanan**.
            - **Tahun ajaran baru** (konsumsi kantin/sekolah).
            - **Pengaruh inflasi umum**.

            ---

            ### 3. Minyak Goreng Kemasan
            - Harga **puncak terjadi di bulan Mei** (sekitar **Rp 24.000**), lalu **turun bertahap hingga akhir tahun**.
            - 🔍 Hal ini mencerminkan dampak dari:
            - **Kebijakan pemerintah terkait subsidi atau HET** (Harga Eceran Tertinggi).
            - Periode di mana subsidi **dicabut lalu dikembalikan**, memengaruhi harga pasar.

            ---

            ### 4. Beras Medium
            - Komoditas **paling stabil sepanjang tahun**, dengan harga berkisar antara **Rp 10.000–Rp 10.500**.
            - Ada **sedikit penurunan di pertengahan tahun**, kemudian **naik lagi di akhir tahun**.
            - 🔍 Stabilitas ini menunjukkan:
            - **Cadangan beras pemerintah cukup kuat**.
            - **Panen relatif terkontrol**, membuat beras tahan terhadap fluktuasi musiman.

            """)
    elif tahun == 2023:
        st.markdown("""
            ## 📈 Insight Tren Harga Komoditas

            ### 1. Daging Ayam Ras
            - Harga **tertinggi terjadi di bulan Juli**, mencapai sekitar **Rp 39.000**.
            - 🔍 Menurut **Ketua Umum Gabungan Organisasi Peternak Ayam Nasional (GOPAN)**, **Herry Darmawan**, penyebab utama kenaikan ini adalah:
            - **Kenaikan harga pakan ayam**, yang memengaruhi biaya produksi di sisi hulu.

            ---

            ### 2. Telur Ayam Ras
            - Mengalami **kenaikan signifikan pada Mei–Juli**, dengan **puncak di bulan Juli (Rp 31.000)**.
            - Setelah itu, harga **turun tajam hingga Oktober**, lalu **stabil di kisaran Rp 26.000–27.000**.
            - 🔍 Menurut **Satgas Pangan Polri**, penyebab utamanya adalah:
            - **Kelangkaan bahan baku pakan ternak**, khususnya **jagung**.
            - **Peningkatan permintaan telur** untuk kebutuhan **program bantuan sosial**.

            ---

            ### 3. Minyak Goreng Kemasan
            - Harga **cenderung stabil sepanjang tahun**, sedikit **turun dari Rp 17.500 ke sekitar Rp 16.300**.
            - 🔍 Hal ini dipengaruhi oleh:
            - **Kebijakan Domestic Market Obligation (DMO)** dari pemerintah untuk menjamin pasokan dalam negeri.
            - Harga **minyak sawit mentah (CPO)** di pasar dunia yang **relatif stabil** selama periode ini.

            ---

            ### 4. Beras Medium
            - Dari **Januari hingga Agustus**, harga beras **relatif stabil**.
            - Namun terjadi **kenaikan signifikan di bulan September**, mencapai sekitar **Rp 17.000**.
            - 🔍 Kenaikan ini kemungkinan disebabkan oleh:
            - **Gagal panen lokal** atau **kekeringan**.
            - **Dampak inflasi** dan **kenaikan harga pupuk** yang meningkatkan biaya produksi petani.

            """)
    elif tahun == 2024:
        st.markdown("""
            ## 📊 Insight Tren Harga Komoditas Berdasarkan Diagram

            ### 1. Daging Ayam Ras
            - Harga **meningkat tajam dari Januari (Rp 34.500) ke Maret (Rp 39.000)**.
            - Setelah itu, harga **menurun bertahap hingga September**.
            - 🔍 Kenaikan di awal tahun kemungkinan disebabkan oleh:
            - **Bulan Ramadhan yang jatuh pada bulan Maret**, meningkatkan konsumsi daging.
            - 🔍 Penurunan harga setelah Maret bisa disebabkan oleh:
            - **Penurunan daya beli masyarakat setelah Lebaran**.
            - **Oversupply (pasokan berlebih)**.
            - **Harga pakan yang stabil atau menurun**.

            ---

            ### 2. Telur Ayam Ras
            - Harga mencapai **puncak di bulan Maret (Rp 31.000)**.
            - 🔍 Kenaikan ini sejalan dengan harga daging ayam, kemungkinan karena:
            - **Bulan puasa** yang meningkatkan konsumsi protein hewani seperti telur.

            ---

            ### 3. Minyak Goreng Kemasan
            - Harga **naik perlahan namun konsisten dari Januari (Rp 16.300) ke Desember (Rp 17.900)**.
            - 🔍 Penyebab utama kenaikan:
            - **Harga minyak sawit mentah (CPO)** sebagai bahan baku utama yang meningkat.
            - **Fluktuasi harga minyak dunia** dan **permintaan global** yang tinggi.

            ---

            ### 4. Beras Medium
            - Terjadi **peningkatan awal tahun (Januari–Maret) dari Rp 13.000 ke Rp 14.300**.
            - Setelah itu, harga **turun kembali dan stabil di kisaran Rp 12.900–13.000**.
            - 🔍 Kenaikan di awal tahun kemungkinan disebabkan oleh:
            - **Permintaan tinggi menjelang Ramadhan**.
            - **Gangguan distribusi** akibat **cuaca ekstrem** atau **masa panen yang belum tiba**.

            """)
    else:
        st.markdown("""
            ## 📈 Insight Tren Harga Komoditas (Januari–Juni)

            ### 1. Daging Ayam Ras
            - Harga **relatif stabil tinggi di Q1 (Januari–Maret)**, berkisar **Rp 36.000–Rp 37.000**.
            - Terjadi **penurunan signifikan mulai April (Rp 34.700) hingga Mei (Rp 33.500)**, lalu **sedikit naik di Juni**.
            - 🔍 Penyebab kemungkinan:
            - **Permintaan tinggi** menjelang **Ramadhan**.
            - Setelah **Idul Fitri**, **permintaan menurun**, menyebabkan harga turun.

            ---

            ### 2. Telur Ayam Ras
            - Harga **stabil di kisaran Rp 27.000–Rp 28.200**.
            - **Penurunan terjadi pada bulan April**, lalu **perlahan naik kembali**.
            - 🔍 Pola ini **mirip dengan daging ayam**, menandakan adanya:
            - **Pengaruh musiman** (Ramadhan dan Lebaran).
            - **Konsumsi tinggi di awal tahun**, lalu berkurang setelahnya.

            ---

            ### 3. Minyak Goreng Kemasan
            - Harga **naik perlahan dari Januari (Rp 19.300) ke Juni (Rp 20.000)**.
            - 🔍 Kenaikan ini didorong oleh:
            - **Naiknya harga minyak sawit mentah (CPO)** sebagai bahan baku utama.
            - **Peningkatan permintaan global**.
            - **Gangguan pasokan**, baik lokal maupun internasional.

            ---

            ### 4. Beras Medium
            - Harga **sangat stabil**, hanya mengalami **kenaikan kecil dari Rp 12.800 ke Rp 13.000**.
            - 🔍 Kenaikan ini kemungkinan disebabkan oleh:
            - **Perubahan musim panen**.
            - **Fluktuasi ringan permintaan dan penawaran** di pasar.

            """)

def run():
    st.subheader('BakoScope - EDA')

    data = pd.read_csv('./src/lib/data_clean_interpolasi.csv')
    data['date'] = pd.to_datetime(data['date'])
    st.dataframe(data)

    #Rata-rata harga tiap komuditas

    st.write('### Rata-rata Harga Tiap Komoditas')
    avg_harga = data.groupby('komoditas')['harga'].mean().reset_index()

    fig = plt.figure(figsize=(10,6))
    sns.barplot(data=avg_harga, x='komoditas', y='harga')

    plt.title('Rata-rata Harga Tiap Komoditas')
    plt.xlabel('Komoditas')
    plt.ylabel('Harga(rata-rata)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)

    st.markdown("""
            ## 📊 Rata-Rata Harga Komoditas

            - **Rata-rata harga Daging Ayam Ras**: Rp **35.582**  
            - **Rata-rata harga Beras Medium**: Rp **11.511**  
            - **Rata-rata harga Minyak Goreng Kemasan**: Rp **17.456**  
            - **Rata-rata harga Telur Ayam Ras**: Rp **26.476**  

            ---

            ## 🔍 Insight

            ### 1. Daging Ayam Ras adalah komoditas dengan harga tertinggi  
            Rata-rata harga daging ayam ras mencapai **Rp35.582**, menunjukkan bahwa komoditas ini memiliki **nilai ekonomis paling tinggi** di antara komoditas lainnya.  
            ➡️ Hal ini bisa disebabkan oleh **biaya produksi dan distribusi** yang lebih besar dibandingkan komoditas lain seperti beras atau telur.

            ### 2. Beras Medium menjadi komoditas dengan harga terendah  
            Rata-rata harga beras medium hanya **Rp11.511**, yang menandakan bahwa **beras sebagai kebutuhan pokok masyarakat** relatif lebih stabil dan terjangkau.  
            ➡️ Ini juga bisa mencerminkan adanya **subsidi atau kontrol harga dari pemerintah**.

            ### 3. Minyak Goreng Kemasan memiliki harga menengah  
            Dengan rata-rata harga **Rp17.456**, minyak goreng berada **di tengah-tengah dalam segi harga**.  
            ➡️ Produk ini sering mengalami fluktuasi harga tergantung pada **pasokan bahan baku seperti kelapa sawit** dan **kondisi pasar global**.

            ### 4. Telur Ayam Ras menunjukkan harga yang kompetitif  
            Telur ayam ras memiliki rata-rata harga **Rp26.476**, menjadikannya **sumber protein yang cukup terjangkau**.  
            ➡️ Telur sering dijadikan **alternatif daging** karena lebih murah dan mudah didistribusikan.
            """)


    st.write('### Tren Rata-rata Komoditas Harga per Tahun')
    data['year'] = data['date'].dt.year
    avg_per_year_komoditas = data.groupby(['year', 'komoditas'])['harga'].mean().reset_index()

    fig = plt.figure(figsize=(8, 6))
    sns.lineplot(data=avg_per_year_komoditas, x='year', y='harga', hue='komoditas', marker='o')

    plt.title('Trend Rata-rata Harga per Komoditas per Tahun')
    plt.xlabel('Tahun')
    plt.ylabel('Harga (Rata-rata)')
    plt.legend(title='Komoditas')
    plt.grid(True)
    plt.tight_layout()
    st.pyplot(fig)
    st.markdown("""
                ## 📈 Insight Tren Rata-rata Harga Komoditas (2021–2025)

                ### 1. Daging Ayam Ras
                - Harga terus **naik dari 2021 hingga puncak di 2024** (sekitar **Rp 36.700**).
                - **Turun ringan di 2025**, namun tetap **lebih tinggi dibanding tahun 2021**.
                - Kemungkinan penyebab:
                - Naiknya biaya **pakan dan produksi**.
                - Stabilnya **permintaan masyarakat** terhadap daging ayam.

                ### 2. Telur Ayam Ras
                - Harga mengalami **kenaikan stabil dari 2021 hingga 2023**, mencapai sekitar **Rp 28.000**.
                - Mulai **sedikit menurun pada 2024 dan 2025**, namun masih **lebih tinggi dari 2021**.
                - Kemungkinan penyebab:
                - Pengaruh **keseimbangan antara pasokan dan permintaan**.

                ### 3. Minyak Goreng Kemasan
                - Terjadi **lonjakan besar dari 2021 ke 2022**, dari sekitar **Rp 15.000-an ke Rp 19.500**.
                - Harga **turun di 2023 dan 2024**, lalu **naik lagi di 2025**.
                - Menunjukkan:
                - Dampak dari **krisis minyak goreng global & lokal**.
                - Tanda-tanda **pemulihan di 2025**.

                ### 4. Beras Medium
                - Terjadi **kenaikan konsisten dari 2021 ke 2024**, dari sekitar **Rp 10.000 ke Rp 13.200**.
                - Ada sedikit **penurunan di 2025**, tapi harga tetap **lebih tinggi dari tahun-tahun awal**.
                - Menunjukkan:
                - Tekanan **inflasi ringan**.
                - Potensi **kenaikan biaya produksi atau distribusi**.
                - Namun, **stabilitas harga tetap terjaga** karena fluktuasi tidak ekstrem.
                """)

    tahun = [2021,2022,2023,2024,2025]
    pilihan = st.selectbox("Pilih tahun :", tahun)  
    st.write(f'### Tren Tahun {pilihan}')
    data_year = data[data['date'].dt.year == pilihan].copy()
    if pilihan == 2021:
        tren_per_month(data_year,pilihan)
    elif pilihan == 2022:
        tren_per_month(data_year,pilihan)
    elif pilihan == 2023:
        tren_per_month(data_year,pilihan)
    elif pilihan == 2024:
        tren_per_month(data_year,pilihan)
    else:
        tren_per_month(data_year,pilihan)
    insight_per_month(pilihan)


if __name__ == '__main__':
    run()
