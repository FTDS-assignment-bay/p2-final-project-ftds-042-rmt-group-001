# BakoScope

## Link Terkait

- **Dashboard Tableau**  
  Visualisasi interaktif untuk analisis harga sembako di [Tableau Dashboard](https://public.tableau.com/app/profile/aris.trisnawan/viz/AnalisaHargaSembako/Dashboard1?publish=yes)

- **Model Prediksi di HuggingFace**  
  Akses prediksi harga sembako secara real-time di [HuggingFace Dashboard](https://huggingface.co/spaces/elangcergasp/bakoscope)

## **[BakoScope](https://huggingface.co/spaces/elangcergasp/bakoscope)** - Forecasting Harga Sembako

Kami adalah tim yang terdiri dari tiga profesional yang bekerja sama untuk memberikan solusi berbasis data dalam analisis harga komoditas sembako di Indonesia. 
Dengan menggunakan berbagai teknik pemodelan data dan eksplorasi analisis, kami berfokus untuk memberikan wawasan yang lebih mendalam terkait fluktuasi harga sembako. 
Dalam proyek ini, kami menerapkan berbagai metode smoothing seperti **Simple Average**, **Moving Average**, **Simple Exponential Smoothing**, **Holt-Linear**, **Holt-Winters**, serta pemodelan **ARIMA** dan **SARIMA**. 

Data yang kami gunakan diperoleh dari **[badanpangan.go.id](https://badanpangan.go.id)**, dan kami memodelkan setiap kombinasi komoditas dan provinsi untuk menghasilkan forecast yang lebih akurat. Kami juga melakukan eksplorasi data dengan memeriksa **seasonality**, **stationarity**, dan **ACF PACF** untuk mendapatkan pemahaman yang lebih baik mengenai pola data yang ada.

## Anggota Tim

1. **Arcana Anggreliya Klau Rissa** – *Data Engineer*  
   Memimpin implementasi dan pengembangan workflow engineering menggunakan **Apache Airflow** untuk melakukan **scraping** data secara periodik dan menjalankan proses **ETL** (Extract, Transform, Load). Bertugas mengintegrasikan sistem dan memastikan alur data yang lancar untuk mempersiapkan data yang siap dianalisis dan dimodelkan.

2. **Aris Trisnawan** – *Data Analyst*  
   Bertanggung jawab atas eksplorasi dan pembersihan data. Memfokuskan pada analisis eksplorasi data (EDA) dan visualisasi time series untuk mengidentifikasi pola harga sembako. Menghasilkan grafik dan dashboard yang dapat digunakan untuk memantau fluktuasi harga serta memberikan pemahaman yang lebih jelas kepada pengguna akhir.

3. **Elang Cergas Pembrani** – *Data Scientist*  
   Mengembangkan dan menerapkan berbagai teknik pemodelan time series termasuk **ARIMA**, **SARIMA**, serta metode smoothing seperti **Simple Exponential Smoothing** dan **Holt-Winters**. Memimpin bagian pengujian model dan analisis terkait **seasonality**, **stationarity**, serta **ACF PACF** untuk setiap kombinasi komoditas dan provinsi.

## Hasil Utama Proyek

- **Analisis EDA pada Time Series Data Harga Sembako**  
  Kami melakukan eksplorasi data mendalam untuk mempelajari karakteristik dan pola harga sembako yang diperoleh dari **badanpangan.go.id**. Temuan utama terkait tren musiman dan fluktuasi harga kami rangkum dalam berbagai visualisasi yang mempermudah pemahaman.

- **Model Forecasting untuk Setiap Kombinasi Komoditas dan Provinsi**  
  Model prediksi harga sembako dikembangkan untuk setiap kombinasi komoditas dan provinsi, yang semuanya dapat diakses secara terintegrasi melalui **dashboard HuggingFace**. Hal ini memungkinkan pengguna untuk melihat dan memantau ramalan harga secara real-time.

- **Workflow Engineering untuk Data Scraping dan ETL**  
  Dengan menggunakan **Apache Airflow**, kami membangun pipeline otomatis untuk melakukan scraping data secara periodik dan proses ETL. Data yang diambil akan diproses menjadi dataset yang siap dianalisis dan dimodelkan, menjamin sistem berjalan dengan efisien dan terkini.

---