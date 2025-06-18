# Proyek Data Pipeline Ekosistem Startup

## Daftar Isi
- [Proyek Data Pipeline Ekosistem Startup](#proyek-data-pipeline-ekosistem-startup)
  - [Daftar Isi](#daftar-isi)
  - [Requirements Gathering \& Solution](#requirements-gathering--solution)
    - [Background Problem](#background-problem)
    - [Proposed solutions](#proposed-solutions)
    - [Profiling Data](#profiling-data)
    - [Design Pipeline](#design-pipeline)
  - [Design Target Database](#design-target-database)
  - [Design of the ETL Pipeline](#design-of-the-etl-pipeline)
  - [Stack or tools or libraries used](#stack-or-tools-or-libraries-used)
  - [How the ETL Pipeline works and how to run it](#how-the-etl-pipeline-works-and-how-to-run-it)
  - [Expected Output for each Process](#expected-output-for-each-process)

---

## Requirements Gathering & Solution

### Background Problem

Di **VenturePulse**, firma penasihat investasi kita, tugas utamanya adalah membantu para Venture Capitalist (VC) menemukan startup "unicorn" berikutnya. Namun, proses kerja kita saat ini sangat bergantung pada data yang terpisah-pisah, sehingga menghambat analisis strategis.

Tim analis kita setiap hari menghadapi tantangan dalam menggabungkan data dari tiga sumber utama:

1.  **Database Internal (PostgreSQL)**: Berisi data investasi dan detail IPO/akuisisi.
2.  **File CSV**: Berisi informasi para individu kunci (pendiri, investor, dewan direksi).
3.  **API Eksternal**: Menyediakan data *milestone* atau pencapaian penting perusahaan.

Kondisi ini membuat proses analisis menjadi lambat dan reaktif. Lebih parahnya, fragmentasi data secara langsung menghambat tiga proses bisnis inti yang krusial bagi VenturePulse:

* **Sulit Mengevaluasi Perjalanan Pendanaan & Pertumbuhan Startup**: Data investasi di database tidak sinkron dengan data *milestone* dari API. Akibatnya, kita tidak bisa menjawab pertanyaan vital seperti, *"Apakah pendanaan Seri B kemarin benar-benar mempercepat ekspansi pasar?"* dengan cepat dan akurat.
* **Analisis Strategi Exit & Kinerja Pasar Tidak Komprehensif**: Informasi tentang akuisisi atau IPO di database terputus dari profil para individu kunci di file CSV. Ini membuat kita mustahil untuk menganalisis pola, seperti *"Apakah startup yang didirikan oleh tim dengan latar belakang teknis cenderung lebih sering diakuisisi?"*.
* **Mustahil Melakukan Pemetaan Ekosistem & Jaringan Secara Efektif**: Informasi tentang "siapa pemain kunci" tersebar. Menghubungkan seorang investor ke rekam jejak investasinya di database dan pencapaian portofolionya dari API adalah pekerjaan manual yang sangat lambat.

### Proposed solutions

Untuk mengatasi masalah tersebut, solusi yang diusulkan adalah membangun sebuah **data pipeline otomatis** sebagai tulang punggung operasional VenturePulse. Pipeline ini akan menarik data dari ketiga sumber tersebut, membersihkannya, mengintegrasikannya, dan menyajikannya dalam satu Data Warehouse yang terpusat.

Tujuannya adalah menciptakan **satu sumber kebenaran (single source of truth)** yang solid. Dengan ini, tim analis bisa beralih dari pekerjaan manual mengolah data mentah menjadi fokus menjawab pertanyaan-pertanyaan strategis yang mendukung proses bisnis inti.

Secara spesifik, solusi ini mencakup beberapa tujuan utama:

* **Integrasi Data**: Menggabungkan data dari database PostgreSQL, file CSV/JSON, dan API eksternal ke dalam satu lokasi terpusat.
* **Transformasi Data**: Membersihkan, menstandarisasi, dan mengubah data mentah menjadi format yang siap dianalisis.
* **Automasi Pipeline**: Membuat workflow ETL/ELT yang bisa jalan sendiri secara terjadwal, jadi data selalu *fresh*.
* **Desain Data Warehouse**: Merancang skema database yang dioptimalkan untuk menjawab pertanyaan-pertanyaan analitik dengan cepat.
* **Dokumentasi**: Membuat panduan yang jelas supaya siapa saja di tim bisa paham arsitektur dan cara menjalankan pipeline ini.

### Profiling Data

Sebelum mulai, kita "intip" dulu kondisi data mentah kita. Dari hasil profiling awal, ditemukan beberapa masalah klasik:

* **Database Investasi**: Di kolom `funding_round_type`, ada penulisan yang tidak konsisten, misalnya `'series_a'` dan `'Series A'`. Kolom `raised_amount_usd` juga banyak yang kosong (null).
* **File CSV Orang**: Kolom `role` juga berantakan, ada yang menulis `'founder'`, ada juga `'Co-Founder'`. Beberapa nama di kolom `full_name` juga kadang kosong.
* **API Milestone**: Tanggal di `milestone_at` masih dalam format teks (string), bukan format tanggal yang standar. Ini akan menyulitkan perhitungan durasi.

Masalah-masalah seperti ini yang akan kita bereskan di dalam pipeline.

### Design Pipeline

Arsitektur pipeline kita akan dibagi menjadi beberapa lapisan agar rapi dan mudah dikelola.

* **Layers (Lapisan)**
    * **Staging Layer**: Ibaratnya ini adalah "ruang transit". Semua data dari sumber (database, CSV, API) kita kumpulkan dulu di sini apa adanya, tanpa diubah-ubah. Tujuannya agar kita punya salinan asli data mentah. Kita akan pakai skema `staging` di PostgreSQL untuk ini.
    * **Warehouse Layer**: Ini adalah "ruang pamer" kita. Data dari *staging* yang sudah dibersihkan, dirapikan, dan digabungkan akan disimpan di sini. Model datanya didesain khusus agar para analis bisa menarik laporan dengan cepat. Kita akan pakai skema `warehouse` di PostgreSQL.
* **Log**
    Setiap kali pipeline berjalan, semua aktivitasnya akan dicatat. Kapan mulai, kapan selesai, berapa data yang diproses, apakah ada error, semuanya akan tercatat di sebuah tabel sederhana bernama `pipeline_logs`. Ini sangat penting untuk memantau kesehatan pipeline dan melacak masalah jika terjadi error.
* **Validation System**
    Untuk memastikan data yang masuk ke *warehouse* berkualitas, kita akan menambahkan sistem validasi. Contohnya, kita akan pastikan bahwa `company_id` tidak boleh kosong atau `raised_amount_usd` tidak boleh bernilai negatif. Validasi ini akan dijalankan sebelum data dimuat ke *warehouse*.

## Design Target Database

Kita akan menggunakan **Star Schema** untuk Data Warehouse. Desain ini memisahkan data menjadi tabel fakta (berisi angka dan pengukuran, seperti jumlah investasi) dan tabel dimensi (berisi deskripsi atau konteks, seperti nama perusahaan atau kategori).

* **Tabel Fakta (Contoh)**: `fact_investments`
* **Tabel Dimensi (Contoh)**: `dim_companies`, `dim_people`, `dim_dates`, `dim_geography`

## Design of the ETL Pipeline

Proses ETL (Extract, Transform, Load) kita akan berjalan sebagai berikut:

1.  **Extract**: Skrip akan mengambil data dari tiga sumber: menarik data dari database PostgreSQL, membaca file CSV terbaru, dan memanggil API milestone. Semua data ini akan dimasukkan ke *Staging Layer*.
2.  **Transform**: Di sinilah "keajaiban" terjadi. Data di *staging* akan kita bersihkan (menangani nilai kosong, menyeragamkan format), lalu kita gabungkan (misalnya menghubungkan ID perusahaan di data investasi dengan ID di data milestone).
3.  **Load**: Data yang sudah bersih dan rapi hasil transformasi kemudian dimuat ke dalam tabel-tabel di *Warehouse Layer*.

## Stack or tools or libraries used

* **Bahasa Pemrograman**: Python
* **Manipulasi Data**: Pandas
* **Database**: PostgreSQL
* **Orkestrasi (Penjadwalan)**: Bisa dimulai dengan `cron` atau jika lebih kompleks menggunakan Apache Airflow / Prefect.
* **Manajemen Environment**: `venv` (Python Virtual Environment)

## How the ETL Pipeline works and how to run it

1.  **Setup Awal**:
    * Pastikan Python dan PostgreSQL sudah terpasang.
    * *Clone* repositori ini.
    * Buat *virtual environment* (`python -m venv venv`) dan aktifkan.
    * Install semua *library* yang dibutuhkan dengan `pip install -r requirements.txt`.
    * Salin file `.env.example` menjadi `.env` dan sesuaikan isinya dengan koneksi database Anda.
2.  **Menjalankan Pipeline**:
    * Cukup jalankan skrip utama dari terminal: `python src/main.py`.
    * Skrip ini akan otomatis menjalankan proses Extract, Transform, dan Load secara berurutan.

## Expected Output for each Process

Setelah pipeline ini berjalan, tim analis di VenturePulse bisa mendapatkan hasil berikut untuk setiap proses bisnis:

* **Output untuk Evaluasi Pendanaan**: Sebuah *dashboard* yang menampilkan tren pendanaan per sektor, korelasi antara *funding rounds* dengan peluncuran produk baru, dan pertumbuhan perusahaan setelah mendapatkan investasi.
* **Output untuk Analisis Strategi Exit**: Laporan yang memetakan jenis-jenis *exit* (IPO vs. Akuisisi) berdasarkan industri, lokasi, dan profil pendiri. Analis bisa dengan mudah menemukan pola-pola yang ada.
* **Output untuk Pemetaan Ekosistem**: Sebuah visualisasi jaringan yang menghubungkan investor, pendiri, dan perusahaan. Ini memungkinkan analis untuk melihat "siapa kenal siapa" dan mengidentifikasi pemain-pemain kunci di ekosistem.