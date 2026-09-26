# Omnichannel Retail Data Pipeline

# link PPT
https://drive.google.com/drive/folders/1r3K3bVTgiDZOZH3nnXWWkCgYw3M3Np-t?usp=sharing

## 1. Project Overview

Project ini membangun **end-to-end data pipeline** untuk perusahaan retail omnichannel yang menjual produk melalui toko fisik dan channel digital serta menjalankan campaign marketing.

Data perusahaan berasal dari berbagai sumber dan belum berada dalam satu sistem yang terintegrasi. Dataset mencakup data operasional, transaksi, event pembayaran, refund, return, customer support, aktivitas web, inventory, serta campaign marketing.

Pipeline dibangun menggunakan arsitektur **Medallion Architecture**:

```text
Raw Data
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Quality Gate & Reconciliation
   ↓
Gold Layer
   ↓
Analytics / NL-to-SQL
```

Pipeline diorkestrasi menggunakan **Apache Airflow**, sedangkan penyimpanan utama menggunakan **PostgreSQL lokal** pada database:

```text
milestone1_omnichannel
```

Tujuan utama pipeline adalah menghasilkan data yang **traceable, typed, normalized, validated, deduplicated, dan analytics-ready** tanpa kehilangan bukti data asli dari source.

---
-----------------------------------------------------------
## Cara Menjalankan Project

## 1. Requirements

Pastikan device sudah memiliki:

- Git
- Python
- UV
- Docker Desktop
- VS Code

Clone repository:

```powershell
git clone <URL_REPOSITORY>
cd milestone1-omnichannel_retail
```

Buka project di VS Code:

```powershell
code .
```

---

## 2. Setup Python Environment

Install dependency project:

```powershell
uv sync
```

Aktifkan virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Di VS Code pilih:

```text
Ctrl + Shift + P
→ Python: Select Interpreter
→ .venv\Scripts\python.exe
```

Untuk notebook pilih kernel `.venv` yang sama.

---

## 3. Konfigurasi `.env`

Buat file `.env` di root project.

---

# 4. Mengambil Data dari Google Drive API

Dataset diambil menggunakan Google Drive API melalui `gdrive.py`.

Folder Google Drive:

```python
FOLDER_ID = "id_gdrivemu"
```

Output:

```text
data/raw/
```

### Aktifkan Google Drive API

Di Google Cloud Console:

```text
Create / pilih Project
→ APIs & Services
→ Library
→ Google Drive API
→ Enable
```

Kemudian:

```text
APIs & Services
→ Credentials
→ Create Credentials
→ OAuth Client ID
→ Desktop App
```

Download OAuth credential dan simpan di root project dengan nama:

```text
credentials.json
```

Install dependency jika belum tersedia:

```powershell
uv add google-api-python-client google-auth google-auth-oauthlib
```

Jalankan:

```powershell
uv run python gdrive.py
```

Browser akan terbuka untuk login Google.

Setelah autentikasi berhasil, script membuat:

```text
token.json
```

dan mengunduh isi folder Google Drive secara recursive ke:

```text
data/raw/
```

Cek hasil:

```powershell
Get-ChildItem .\data\raw -Recurse -File
```

---

# 5. Menjalankan PostgreSQL dan Airflow

Pastikan Docker Desktop aktif.

Build image Airflow:

```powershell
docker build -t airflow-spark .
```

Jalankan environment:

```powershell
docker compose -f airflow.yaml up -d
```

Cek container:

```powershell
docker compose -f airflow.yaml ps
```

Buka Airflow:

```text
http://localhost:8080
```

Login:

```text
Username : a******
Password : a******
```

Cari DAG:

```text
omnichannel_retail_pipeline
```

Kemudian klik:

```text
Trigger DAG
```

Pipeline menjalankan:

```text
create_pipeline_run
        ↓
validate_raw_files
        ↓
initialize_postgresql_schemas
        ↓
load_bronze
        ↓
build_silver
        ↓
silver_quality_gate
        ↓
build_gold
        ↓
validate_gold_and_record_metadata
```

---

# 6. Menjalankan Notebook

Pastikan `.venv` sudah menjadi kernel VS Code.

Buka file notebook:

```text
*.ipynb
```

Pilih:

```text
Select Kernel
→ Python Environments
→ .venv
```

Kemudian jalankan:

```text
Run All
```

Notebook digunakan untuk pengecekan atau eksplorasi pipeline. Pipeline end-to-end utama tetap dijalankan melalui Airflow.

---

# 7. Mengecek Database

Database pipeline:

```text
milestone1_omnichannel
```

Contoh koneksi PostgreSQL dari host:

```text
Host     : localhost
Port     : 5432
Database : milestone1_omnichannel
Username : 
Password : 
```

Cek Bronze:

```sql
SELECT COUNT(*)
FROM bronze.raw_records;
```

Cek rejected Silver:

```sql
SELECT COUNT(*)
FROM silver.rejected_records;
```

Cek Gold:

```sql
SELECT COUNT(*) FROM gold.order_360;
SELECT COUNT(*) FROM gold.customer_daily;
SELECT COUNT(*) FROM gold.product_daily;
SELECT COUNT(*) FROM gold.channel_campaign_daily;
SELECT COUNT(*) FROM gold.executive_kpis_daily;
```

---

# 8. Menjalankan NL-to-SQL Engine

Pastikan `.env` berisi:

```env
NL2SQL_PROVIDER=openai
OPENAI_API_KEY=YOUR_OPENAI_API_KEY
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4.1-mini
```

Untuk engine yang dijalankan dari host, database menggunakan:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=
DB_PASSWORD=
DB_NAME=milestone1_omnichannel
```

Jalankan FastAPI engine dari root project:

```powershell
uv run uvicorn engine.main:app --reload
```

Engine menggunakan Gold sebagai sumber analytics:

```text
gold.*
```

Jika UI project digunakan, buka terminal baru dan jalankan UI sesuai entrypoint pada folder `ui`.

---

# 9. Urutan Menjalankan Project di Device Baru

```text
1. Clone repository
        ↓
2. Buka project di VS Code
        ↓
3. uv sync
        ↓
4. Pilih interpreter .venv
        ↓
5. Buat .env
        ↓
6. Tambahkan credentials.json
        ↓
7. Jalankan gdrive.py
        ↓
8. Pastikan data/raw terisi
        ↓
9. Aktifkan Docker Desktop
        ↓
10. docker build -t airflow-spark .
        ↓
11. docker compose -f airflow.yaml up -d
        ↓
12. Trigger DAG Airflow
        ↓
13. Pastikan Bronze → Silver → Gold sukses
        ↓
14. Jalankan NL-to-SQL Engine
        ↓
15. Jalankan UI / analytics
```

---

# 10. Setelah Selesai

Stop Airflow:

```powershell
docker compose -f airflow.yaml down
```

Keluar dari virtual environment:

```powershell
deactivate
```

## Pipeline Flow

```text
Google Drive API
        ↓
gdrive.py
        ↓
data/raw
        ↓
Airflow
        ↓
Bronze
        ↓
Silver
        ↓
Quality Gate
        ↓
Gold
        ↓
NL-to-SQL Engine
        ↓
Analytics / UI
```
-------------------------------

# 2. Business Scenario

Perusahaan retail omnichannel memiliki beberapa jenis data yang berasal dari sistem berbeda.

Data tersebut meliputi:

- customer dan customer profile;
- product dan product category;
- store dan sales channel;
- order dan order item;
- promotion;
- payment;
- refund;
- return;
- customer support;
- aktivitas web;
- inventory;
- campaign marketing.

Kondisi source data tidak selalu bersih. Dataset dapat mengandung:

- duplicate records;
- invalid data type;
- missing reference;
- invalid quantity;
- event yang datang terlambat;
- event yang tidak berurutan;
- perbedaan format timestamp;
- risiko fact multiplication ketika beberapa tabel event digabungkan.

Oleh karena itu, data tidak langsung digunakan untuk analisis.

Pipeline dibangun untuk membawa data melalui beberapa tahap pengolahan sebelum menghasilkan business metrics yang dapat digunakan untuk analisis.

---

# 3. Data Sources

Pipeline menerima data dalam format **CSV dan JSON**.

Dataset terdiri dari **19 source files**:

```text
customers
customer_profiles
customer_addresses
products
product_categories
stores
sales_channels
promotions
orders
order_items
order_promotions
payment_events
refund_events
return_events
support_events
web_events
inventory_snapshots.csv
campaign_spend.csv
city_reference.json
```

Selain source data tersebut, terdapat:

```text
manifest.json
```

`manifest.json` digunakan untuk memvalidasi bahwa file yang dibutuhkan tersedia sebelum proses ingestion dijalankan.

Source data ditempatkan pada:

```text
data/raw/
```

---

# 4. Pipeline Architecture

Arsitektur pipeline:

```text
                CSV / JSON
                    │
                    ▼
               data/raw/
                    │
                    ▼
        ┌──────────────────────┐
        │     BRONZE LAYER     │
        │   bronze.raw_records │
        └──────────────────────┘
                    │
                    ▼
        ┌──────────────────────┐
        │     SILVER LAYER     │
        │ Typed                │
        │ Normalized           │
        │ Validated            │
        │ Deduplicated         │
        │ Lineage              │
        └──────────────────────┘
                    │
                    ▼
        ┌──────────────────────┐
        │     QUALITY GATE     │
        │ Quality Checks       │
        │ Reconciliation       │
        └──────────────────────┘
                    │
                    ▼
        ┌──────────────────────┐
        │      GOLD LAYER      │
        │ Business Metrics     │
        │ Fixed Grain          │
        └──────────────────────┘
                    │
                    ▼
             Analytics /
              NL-to-SQL
```

Setiap layer memiliki tanggung jawab berbeda.

**Bronze** mempertahankan bukti data source.

**Silver** membersihkan dan menstandarkan data.

**Gold** menghasilkan dataset dan metric yang siap digunakan untuk kebutuhan bisnis.

---

# 5. Bronze Layer

## Tujuan

Bronze berfungsi sebagai **immutable source evidence**.

Data source tidak langsung dibersihkan atau diubah menjadi business table. Payload asli disimpan terlebih dahulu sehingga data dapat ditelusuri kembali ke sumbernya.

Tabel utama:

```sql
bronze.raw_records
```

## Data yang Disimpan

Bronze menyimpan informasi seperti:

```text
raw_record_id
pipeline_run_id
ingested_at_utc
source_file
source_row_number
source_record_id
record_checksum
raw_payload
```

`raw_payload` disimpan dalam bentuk **JSONB** sehingga struktur asli record tetap dapat dipertahankan.

## Lineage

Beberapa metadata disimpan untuk menjaga traceability.

### `pipeline_run_id`

Mengidentifikasi pipeline execution yang memasukkan record.

### `ingested_at_utc`

Mencatat waktu record masuk ke pipeline menggunakan UTC.

### `source_file`

Menunjukkan file asal record.

### `source_row_number`

Menunjukkan posisi record pada source.

### `source_record_id`

Menyimpan identifier record dari source jika tersedia.

### `record_checksum`

Checksum dibuat menggunakan **SHA-256** untuk membantu mengidentifikasi record yang sama dan mendukung proses deduplication/idempotency.

Dengan metadata tersebut, record dapat ditelusuri:

```text
Gold
 ↓
Silver
 ↓
Bronze
 ↓
Source File
```

---

# 6. Silver Layer

Silver merupakan layer utama untuk proses **data engineering dan data quality**.

Silver **tidak membaca langsung CSV/JSON**.

Silver hanya membaca:

```text
bronze.raw_records
```

Hal ini menjaga layering agar setiap tahap memiliki tanggung jawab yang jelas.

## Proses Silver

Silver melakukan:

```text
Parsing
   ↓
Type Casting
   ↓
Normalization
   ↓
Timestamp → UTC
   ↓
Validation
   ↓
Deterministic Deduplication
   ↓
Referential Integrity Check
   ↓
Event Validation
   ↓
Lineage
   ↓
Rejected Records Handling
```

---

## 6.1 Typed Data

Data yang sebelumnya berada dalam `raw_payload` dikonversi menjadi tipe data yang sesuai.

Contoh:

```text
"quantity": "2"
```

diubah menjadi:

```text
quantity = 2
```

dengan tipe integer.

Hal yang sama dilakukan pada:

- numeric values;
- date;
- timestamp;
- identifier;
- boolean;
- categorical attributes.

---

## 6.2 Timestamp Normalization

Timestamp dari source dapat memiliki format atau timezone yang berbeda.

Silver melakukan normalisasi seluruh timestamp ke:

```text
UTC
```

Tujuannya agar event dari sistem berbeda dapat dibandingkan menggunakan referensi waktu yang sama.

---

## 6.3 Deduplication

Silver melakukan **deterministic deduplication**.

Artinya, ketika terdapat beberapa record untuk entity/event yang sama, pipeline memiliki aturan tetap untuk menentukan record yang dipertahankan.

Hal ini membuat hasil pipeline konsisten ketika dijalankan kembali.

---

## 6.4 Referential Integrity

Silver memeriksa hubungan antar-entity.

Contoh:

```text
order.customer_id
```

harus memiliki customer yang valid.

Begitu juga hubungan seperti:

```text
order_item → order
order_item → product
order → channel
order → store
promotion → order
```

Record dengan reference yang tidak valid dapat ditandai atau dimasukkan ke mekanisme rejection sesuai rule pipeline.

---

## 6.5 Event Handling

Dataset memiliki berbagai event:

```text
payment
refund
return
support
web/mobile activity
```

Silver menangani:

- event identity;
- duplicate event;
- event ordering;
- temporal consistency;
- late-arriving event.

Hal ini penting karena event tidak selalu masuk sesuai urutan waktu terjadinya.

---

## 6.6 Rejected Records

Data yang tidak memenuhi aturan validasi tidak boleh diam-diam masuk ke dataset analytics.

Record bermasalah dipisahkan melalui mekanisme **rejected records** sehingga masalah tetap dapat ditelusuri.

Dengan demikian:

```text
valid record   → Silver table
invalid record → rejection handling
```

---

# 7. Silver Quality Gate

Sebelum Gold dibangun, pipeline menjalankan **Silver Quality Gate**.

Tujuannya memastikan data Silver sudah memenuhi standar minimum kualitas.

Pemeriksaan mencakup:

- duplicate records;
- null pada mandatory field;
- invalid data type;
- invalid identifier;
- invalid quantity/value;
- referential integrity;
- timestamp consistency;
- event consistency;
- rejected records;
- row-count reconciliation.

Konsepnya:

```text
Silver
   │
   ▼
Quality Gate
   │
   ├── FAIL → Gold tidak dilanjutkan
   │
   └── PASS
          │
          ▼
        Gold
```

Quality gate mencegah data bermasalah langsung digunakan sebagai business metric.

---

# 8. Reconciliation

Pipeline melakukan reconciliation untuk membuktikan bahwa transformasi tidak menyebabkan kehilangan atau penggandaan data yang tidak dapat dijelaskan.

Contoh konsep reconciliation:

```text
Source/Bronze Count
        ↓
Silver Valid Records
        +
Rejected Records
        ↓
Expected Record Count
```

Reconciliation juga digunakan untuk mendeteksi risiko **fact multiplication**.

Hal ini penting ketika order digabungkan dengan beberapa one-to-many event seperti:

```text
order
 ├── order_items
 ├── payments
 ├── refunds
 └── returns
```

Jika seluruh tabel tersebut langsung di-join, satu order dapat muncul berkali-kali dan menyebabkan revenue terhitung lebih besar dari nilai sebenarnya.

Karena itu, event/fact perlu diagregasi sesuai grain sebelum digunakan untuk membangun metric Gold.

---

# 9. Gold Layer

Gold merupakan layer **analytics-ready**.

Gold hanya membaca data dari:

```text
silver.*
```

Gold tidak membaca langsung Bronze maupun raw source.

Setiap tabel Gold memiliki **grain dan metric definition yang tetap**.

Tabel Gold yang dibangun:

```text
gold.order_360
gold.customer_daily
gold.product_daily
gold.channel_campaign_daily
gold.executive_kpis_daily
```

---

## 9.1 `gold.order_360`

**Grain:**

```text
1 row = 1 order
```

Tabel ini memberikan consolidated view terhadap sebuah order.

Informasi dapat mencakup:

- order;
- customer;
- channel;
- payment;
- refund;
- return;
- revenue.

Tujuannya adalah menghindari kebutuhan melakukan join kompleks terhadap banyak event table setiap kali melakukan analisis order.

---

## 9.2 `gold.customer_daily`

**Grain:**

```text
1 row = 1 customer per date
```

Digunakan untuk melihat aktivitas dan performa customer dari waktu ke waktu.

---

## 9.3 `gold.product_daily`

**Grain:**

```text
1 row = 1 product per date
```

Digunakan untuk menganalisis performa produk seperti transaksi, quantity, revenue, return, dan inventory-related metric.

---

## 9.4 `gold.channel_campaign_daily`

**Grain:**

```text
1 row = 1 date × channel × campaign
```

Digunakan untuk menghubungkan performa penjualan dengan aktivitas marketing.

Metric penting yang dapat dihitung antara lain:

```text
campaign spend
revenue
conversion-related metrics
ROAS
```

---

## 9.5 `gold.executive_kpis_daily`

**Grain:**

```text
1 row = 1 date
```

Tabel ini menyediakan ringkasan KPI harian untuk kebutuhan monitoring bisnis.

Metric yang dibangun pada Gold mencakup antara lain:

```text
net_revenue
refund_rate
return_rate
ROAS
stockout
AOV
```

---

# 10. Metric Definitions

## Net Revenue

Secara konseptual:

```text
Net Revenue = Revenue - Refund
```

Metric ini digunakan agar performa bisnis tidak hanya melihat gross transaction value tetapi mempertimbangkan refund.

---

## Refund Rate

Digunakan untuk mengukur proporsi transaksi/nilai yang mengalami refund berdasarkan definisi grain metric yang digunakan.

---

## Return Rate

Digunakan untuk mengetahui tingkat produk/order yang dikembalikan.

Metric ini dapat membantu mengidentifikasi pola return pada product, category, atau channel.

---

## Average Order Value — AOV

Secara umum:

```text
AOV = Revenue / Number of Orders
```

Digunakan untuk melihat rata-rata nilai transaksi.

---

## ROAS

```text
ROAS = Revenue Attributed to Campaign / Campaign Spend
```

Digunakan untuk membandingkan hasil revenue dengan biaya campaign.

---

## Stockout

Inventory snapshot digunakan untuk mengidentifikasi kondisi ketika inventory produk mencapai kondisi stockout.

---

# 11. Apache Airflow Orchestration

Pipeline diorkestrasi menggunakan **Apache Airflow**.

Airflow digunakan agar setiap proses berjalan sesuai dependency dan tidak perlu dieksekusi secara manual satu per satu.

Urutan pipeline:

```text
create_pipeline_run
        ↓
validate_manifest_and_raw_files
        ↓
initialize_postgresql_schemas
        ↓
load_bronze
        ↓
build_silver
        ↓
silver_quality_gate
        ↓
build_gold
        ↓
validate_gold_and_record_metadata
```

## `create_pipeline_run`

Membuat identifier untuk satu pipeline execution.

Identifier tersebut digunakan untuk monitoring dan lineage.

---

## `validate_manifest_and_raw_files`

Memastikan:

---
- source file tersedia;
- file yang diperlukan lengkap sebelum ingestion dimulai.

---

## `initialize_postgresql_schemas`

Mempersiapkan schema dan object database yang diperlukan oleh pipeline.

Schema utama:

```text
bronze
silver
gold
ops
```

---

## `load_bronze`

Melakukan ingestion raw CSV/JSON ke:

```text
bronze.raw_records
```

beserta metadata lineage dan checksum.

---

## `build_silver`

Melakukan:

```text
typing
normalization
validation
deduplication
referential checks
event handling
lineage
rejection handling
```

---

## `silver_quality_gate`

Memastikan hasil Silver memenuhi data-quality requirement sebelum Gold boleh dibangun.

---

## `build_gold`

Membangun tabel analytics:

```text
order_360
customer_daily
product_daily
channel_campaign_daily
executive_kpis_daily
```

---

## `validate_gold_and_record_metadata`

Melakukan final validation terhadap hasil Gold dan mencatat metadata/status pipeline execution.

---

# 12. Pipeline Rerun & Idempotency

Pipeline dirancang agar dapat dijalankan kembali tanpa menghasilkan duplikasi yang tidak terkontrol.

Mekanisme yang mendukung rerun antara lain:

```text
pipeline_run_id
record_checksum
deterministic deduplication
fixed transformation rules
quality checks
reconciliation
```

Setiap pipeline execution tetap memiliki identitas tersendiri sehingga proses dapat diaudit.

---

# 13. Operational Monitoring

Pipeline menggunakan schema:

```text
ops
```

untuk membantu pencatatan informasi operasional pipeline.

Informasi tersebut dapat digunakan untuk mengetahui:

```text
pipeline run
status
execution
error
metadata
```

Dengan demikian kegagalan pipeline dapat ditelusuri ke tahap yang menyebabkan masalah.

---

# 14. Technology Stack

Teknologi utama:

```text
Python
PostgreSQL 16
Apache Airflow
Docker
SQL
Pandas / data-processing utilities
Jupyter Notebook
```

Arsitektur penyimpanan utama:

```text
Local PostgreSQL
        │
        ├── bronze
        ├── silver
        ├── gold
        └── ops
```

Bronze dan Silver **tidak dikirim ke Neon DB**.

---

# 15. Why Bronze, Silver, and Gold?

Pipeline tidak langsung mengubah raw data menjadi Gold karena setiap layer memiliki tanggung jawab berbeda.

### Bronze — Preserve

```text
"What exactly did we receive?"
```

Bronze mempertahankan data asli dan lineage.

### Silver — Trust

```text
"Can this data be trusted and joined safely?"
```

Silver melakukan typing, cleaning, validation, normalization, deduplication, dan referential checking.

### Gold — Serve

```text
"What does the business need to analyze?"
```

Gold menghasilkan dataset dengan grain dan metric definition yang jelas.

Pemisahan ini membuat pipeline lebih:

- auditable;
- maintainable;
- traceable;
- testable;
- rerunnable;
- reliable untuk analytics.

---

# 16. Key Engineering Challenges

Beberapa masalah utama yang ditangani pipeline adalah:

### Duplicate Records

Diatasi menggunakan deterministic deduplication dan record identity.

### Different Data Types

Diatasi melalui explicit type casting di Silver.

### Different Timestamp Formats

Dinormalisasi ke UTC.

### Missing References

Dideteksi melalui referential integrity validation.

### Invalid Records

Dipisahkan melalui rejection handling.

### Late-Arriving Events

Ditangani melalui event-time dan temporal processing.

### Fact Multiplication

Dicegah dengan menjaga grain dan melakukan aggregation sebelum menggabungkan beberapa one-to-many fact/event.

### Pipeline Reliability

Dijaga menggunakan quality gate dan reconciliation sebelum Gold digunakan.

---

# 17. Final Pipeline Flow

Secara keseluruhan:

```text
19 CSV / JSON Source Files
          │
          ▼
     manifest.json
          │
          ▼
   Validate Raw Files
          │
          ▼
┌─────────────────────┐
│       BRONZE        │
│ Raw + JSONB         │
│ Checksum            │
│ Source Metadata     │
│ Pipeline Lineage    │
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│       SILVER        │
│ Typed               │
│ Normalized          │
│ UTC Timestamp       │
│ Validated           │
│ Deduplicated        │
│ Referential Checks  │
│ Rejected Records    │
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│    QUALITY GATE     │
│ Data Quality Checks │
│ Reconciliation      │
└─────────────────────┘
          │
        PASS
          │
          ▼
┌─────────────────────┐
│        GOLD         │
│ order_360           │
│ customer_daily      │
│ product_daily       │
│ channel_campaign    │
│ executive_kpis      │
└─────────────────────┘
          │
          ▼
   Analytics / NL-to-SQL
```

---

# 18. Conclusion

Pipeline ini mengubah data retail omnichannel yang berasal dari berbagai CSV dan JSON menjadi dataset analytics-ready dengan menerapkan arsitektur Bronze, Silver, dan Gold.

Bronze mempertahankan raw source dan lineage, Silver memastikan data telah typed, normalized, validated, dan deduplicated, sedangkan Gold menyediakan tabel dengan grain dan business metric yang jelas.

Quality gate dan reconciliation ditempatkan sebelum Gold untuk memastikan hasil analytics tidak dibangun dari data yang belum dapat dipercaya.

Apache Airflow mengorkestrasi seluruh proses sehingga pipeline dapat dijalankan secara terstruktur, dimonitor, ditelusuri, dan dijalankan kembali secara konsisten.