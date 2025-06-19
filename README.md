# Proyek Data Pipeline: Startup Ecosystem Analytics

Sebuah data pipeline end-to-end untuk mengintegrasikan, memproses, dan menganalisis data ekosistem startup dari berbagai sumber. Proyek ini dibangun untuk menciptakan satu sumber kebenaran (*single source of truth*) yang memungkinkan analisis mendalam terhadap tren investasi, kinerja perusahaan, dan jaringan para pemain kunci.

## Daftar Isi
- [Proyek Data Pipeline: Startup Ecosystem Analytics](#proyek-data-pipeline-startup-ecosystem-analytics)
  - [Daftar Isi](#daftar-isi)
  - [Requirements Gathering \& Solution](#requirements-gathering--solution)
    - [Latar Belakang Masalah (Background Problem)](#latar-belakang-masalah-background-problem)
    - [Latar Belakang Masalah (Background Problem)](#latar-belakang-masalah-background-problem-1)
      - [1. Ketidakmampuan Mengevaluasi Momentum Pertumbuhan Secara Akurat](#1-ketidakmampuan-mengevaluasi-momentum-pertumbuhan-secara-akurat)
      - [2. Analisis Strategi *Exit* yang Terfragmentasi](#2-analisis-strategi-exit-yang-terfragmentasi)
      - [3. Keterbatasan dalam Pemetaan Jaringan Modal Manusia](#3-keterbatasan-dalam-pemetaan-jaringan-modal-manusia)
    - [Solusi yang Diusulkan (Proposed Solution)](#solusi-yang-diusulkan-proposed-solution)
    - [Temuan Awal dari Profiling Data](#temuan-awal-dari-profiling-data)
    - [Desain Arsitektur Pipeline](#desain-arsitektur-pipeline)
  - [Desain Target Database (Data Warehouse)](#desain-target-database-data-warehouse)
  - [Desain Alur Kerja ETL](#desain-alur-kerja-etl)
  - [Teknologi yang Digunakan](#teknologi-yang-digunakan)
  - [Cara Menjalankan Pipeline](#cara-menjalankan-pipeline)
  - [Hasil yang Diharapkan dari Setiap Analisis](#hasil-yang-diharapkan-dari-setiap-analisis)

## Requirements Gathering & Solution

### Latar Belakang Masalah (Background Problem)
Perusahaan **"VenturePulse"** adalah perusahaan konsultan investasi yang mempunyai klien dari berbagai perusahaan startup hingga institusi keuangan. Dalam menjalankan misinya untuk memberikan wawasan strategis berbasis data, VenturePulse menghadapi kendala utama dalam mengintegrasikan dan menganalisis informasi lintas sumber secara menyeluruh. Keterbatasan akses terhadap data yang tersebar di berbagai format dan lokasi telah menyebabkan sejumlah tantangan kritis berikut:

### Latar Belakang Masalah (Background Problem)

#### 1. Ketidakmampuan Mengevaluasi Momentum Pertumbuhan Secara Akurat

- **Kondisi:** Data pendanaan (`funding_rounds`, `investments`, `funds`) dan pencapaian (`milestones`) tersedia dari berbagai sumber, namun belum dihubungkan secara eksplisit dalam model analitik.
- **Masalah:** Sulit untuk menilai dampak langsung dari pendanaan terhadap pertumbuhan startup. Pertanyaan seperti “apakah pendanaan Seri B mendorong peluncuran produk utama?” tidak dapat dijawab secara langsung karena tidak adanya keterkaitan yang jelas antara waktu, sumber dana, dan pencapaian bisnis.

**Tabel kunci:**
- `funding_rounds`, `investments`, `funds`, `milestones`

---

#### 2. Analisis Strategi *Exit* yang Terfragmentasi

- **Kondisi:** Data akuisisi (`acquisitions`) dan IPO (`ipos`) tersedia dalam database, namun belum dilengkapi dengan atribut deskriptif perusahaan atau waktu yang mendetail untuk mendukung analisis longitudinal dan sektoral.
- **Masalah:** Tanpa model data yang terstruktur untuk membandingkan aktivitas dan nilai exit, sulit melakukan analisis perbandingan antar industri, waktu, atau jenis strategi exit. Pertanyaan seperti “berapa rata-rata valuasi IPO di sektor fintech dalam 5 tahun terakhir” atau “korporasi mana yang paling sering mengakuisisi startup” tidak bisa dijawab secara efisien.

**Tabel kunci:**
- `acquisitions`, `ipos`,  `ipos`

---

#### 3. Keterbatasan dalam Pemetaan Jaringan Modal Manusia

- **Kondisi:** Data `relationships` yang menghubungkan individu ke perusahaan tersedia, dan pencapaian perusahaan (`milestones`) juga tersedia, namun belum terintegrasi dalam satu model analitik untuk pelacakan karier dan inovasi.
- **Masalah:** Sulit melacak jejak kontribusi individu terhadap pertumbuhan dan inovasi lintas perusahaan. Visualisasi jaringan atau analisis dampak modal manusia terhadap performa startup tidak dapat dilakukan secara utuh karena keterbatasan keterkaitan antara individu, peran, dan hasil nyata yang dicapai.

**Tabel kunci:**
- `relationships`, `milestones`, `people`, `company`


### Solusi yang Diusulkan (Proposed Solution)

Oleh karena itu perlu dibangun **Data Pipeline Terpusat** yang mengotomatisasi proses pengumpulan, pembersihan, transformasi, dan pemuatan data dari berbagai sumber ke dalam sebuah **Data Warehouse** tunggal.

Tujuannya adalah menyediakan data yang andal, terintegrasi, dan siap pakai untuk analisis strategis tanpa intervensi manual berlebihan.

### Temuan Awal dari Profiling Data

Proses profiling data mengungkapkan isu kualitas data yang spesifik dan menjadi justifikasi utama untuk setiap langkah dalam proses transformasi ETL. Temuan ini secara langsung berdampak pada kemampuan untuk melakukan analisis bisnis yang diharapkan.

**1. Kelengkapan Data (Completeness) yang Rendah pada Metrik Kunci**

Banyak kolom krusial untuk analisis bisnis memiliki persentase *missing values* yang sangat tinggi. Hal ini secara langsung menghambat proses bisnis yang telah didefinisikan:

* **Dampak pada Analisis Pendanaan & Exit:**
    * Pada tabel `funding_rounds`, data valuasi sangat tidak lengkap, dengan **48.1%** nilai hilang pada `pre_money_currency_code` dan **41.63%** pada `post_money_currency_code`.
    * Pada tabel `acquisitions`, **81.67%** data `term_code` (tipe akuisisi: cash/stock) hilang.
    * **Implikasi:** Tanpa data ini, **mustahil** untuk mengevaluasi kinerja pendanaan atau menganalisis strategi *exit* secara komprehensif. Proses ETL harus menerapkan strategi untuk menangani nilai-nilai yang hilang ini sebelum memuatnya ke `fact_investment_round_participation` dan `fact_acquisitions`. 
      * Pada tabel `funding_rounds`, kolom  `post_money_currency_code` dan `pre_money_currency_code` akan kita drop karena kolom `pre_money_valuation_usd` dan kolom `post_money_valuation_usd` datanya tidak kosong dan sudah dalam usd sehingga tidak perlukan lagi kedua kolom mata tersebut.
      * Pada tabel `acquisitions` kolom `term_code` akan diisi `Unknown` untuk data yang hilang

* **Dampak pada Pemetaan Jaringan & Karier:**
    * Tabel `relationship` memiliki data tanggal yang sangat minim: `start_at` hilang **53.48%** dan `end_at` hilang **85.71%**.
    * **Implikasi:** Ini secara fundamental merusak kemampuan untuk "Memetakan Jaringan Modal Manusia". Analisis durasi karier atau periode aktif seseorang di sebuah perusahaan menjadi tidak akurat. Transformasi pada `fact_relationship` harus mampu menangani tanggal yang kosong ini.
      * Oleh karena itu kita akan mengisi kedua kolom ini dengan tanggal jauh di masa depan (2100-01-01) untuk menandakan tanggal ini kosong. Sehingga bisa difilter saat analisis nantinya.

* **Dampak pada Analisis Geografis:**
    * Informasi lokasi pada tabel `company` juga tidak lengkap, seperti `state_code` (**42.36%** hilang) dan `city` (**4.59%** hilang).
    * **Implikasi:** Analisis berbasis lokasi menjadi kurang andal. Kolom-kolom ini perlu dibersihkan sebelum dimuat ke `dim_company`.

**2. Integritas dan Format Data yang Belum Standar**

* **Tipe Data Tanggal:** Sebagian besar kolom tanggal di berbagai tabel (`funding_rounds.funded_at`, `acquisitions.acquired_at`, `relationship.start_at`) ditemukan sebagai tipe data `object` (string), bukan `datetime`.
* **Implikasi:** Ini menyebabkan semua bentuk analisis berbasis waktu (tren, durasi, dsb.) terhambat. Oleh karena itu semua kolom tanggal harus dikonversi ke format `YYYYMMDD` sebagai *foreign key* ke `dim_date`.

### Desain Arsitektur Pipeline

Pipeline terdiri dari beberapa komponen utama:

- **Layers:**
  - **Data Sources:** PostgreSQL, file CSV/JSON, dan API eksternal.
  - **Staging Layer:** Menyimpan data mentah di PostgreSQL schema `staging`.
  - **Warehouse Layer:** Menyimpan data terstruktur dalam schema `warehouse` berbasis Star Schema.

- **Logging & Monitoring:**
  - Informasi setiap proses etl setiap tabel akan disimpan di tabel database `log` tabel `etl_log`.

- **Validation & Error Handling:**
  - Setiap proses yang error datanya akan disimpan di **minio**. Report hasil hasil validasi juga disimpan di **Minio**.

![Pipeline Design](https://i.imgur.com/7g2tVjQ.png)

## Desain Target Database (Data Warehouse)

Struktur database menggunakan pendekatan **Kimball's Star Schema**.

- **Fakta:**
  - `fact_investment_round_participation`
  - `fact_acquisitions`
  - `fact_ipos`
  - `fact_funds`
  - `fact_milestones`
  - `fact_relationship`
- **Dimensi:**
  - `dim_company`
  - `dim_people`
  - `dim_date`

Pemetaan source-to-target disediakan dalam dokumen terpisah.

## Desain Alur Kerja ETL

1. **Extract:** PySpark menarik data dari sumber dan menyimpannya ke *Staging Layer* PostgreSQL.
2. **Transform:**
   - Pembersihan data (null, duplikat)
   - Standarisasi format
   - Enrichment kolom
   - Integrasi tabel berdasarkan `object_id`
3. **Load:** Data hasil transformasi dimuat ke warehouse secara *incremental* (insert/update).

## Teknologi yang Digunakan

- **Bahasa:** Python
- **Engine Pemrosesan:** Apache Spark (via PySpark)
- **Penyimpanan:**
  - PostgreSQL (Staging & Warehouse)
  - Minio (data profiling dan data hasil validasi)
- **Orkestrasi & Containerization:** Docker, Docker Compose

## Cara Menjalankan Pipeline

1. **Prasyarat:**
   - Docker & Docker Compose
2. **Langkah Setup:**
   ```bash
   git clone <url-repo>
   cd <nama-repo>
   cp .env.example .env  # lalu sesuaikan jika perlu
3.  **Membangun dan Menjalankan Services**:
    * Jalankan perintah berikut untuk membangun image dan menjalankan semua service (Spark, PostgreSQL, Minio) di background:
        ```bash
        docker-compose up -d --build
        ```
4.  **Memicu ETL Job**:
    * Untuk menjalankan pipeline, eksekusi skrip Spark menggunakan `spark-submit` di dalam container `spark-master`:
        ```bash
        docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /app/src/main.py
        ```

## Hasil yang Diharapkan dari Setiap Analisis

Setelah pipeline berjalan dan data warehouse terisi, tim analis VenturePulse akan mampu menjawab pertanyaan-pertanyaan strategis dengan cepat:

* **Untuk Evaluasi Pendanaan & Pertumbuhan:**
    * Membuat dashboard interaktif yang menampilkan tren pendanaan (QoQ, YoY) berdasarkan sektor dan geografi.
    * Menganalisis korelasi langsung antara peristiwa pendanaan dengan peluncuran produk atau *milestone* penting lainnya.

* **Untuk Analisis Strategi Exit:**
    * Menghasilkan laporan yang membandingkan valuasi dan frekuensi IPO vs. Akuisisi di berbagai industri.
    * Mengidentifikasi profil pendiri atau karakteristik perusahaan yang paling sering berujung pada *exit* yang sukses.

* **Untuk Pemetaan Ekosistem:**
    * Membuat visualisasi jaringan yang memetakan hubungan "investor-perusahaan-pendiri".
    * Mengidentifikasi pemain kunci dan "super-connectors" dalam ekosistem startup.