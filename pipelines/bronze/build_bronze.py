# ============================================================
# BRONZE LAYER - OMNICHANNEL RETAIL
# ============================================================

import csv
import hashlib
import json
import uuid
import os

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import URL


# ============================================================
# 1. CONFIGURATION
# ============================================================

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "milestone1_omnichannel")

POSTGRES_SERVER_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database="postgres",
)

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

RAW_DIR = Path(
    os.getenv("RAW_DIR", "/opt/airflow/data/raw")
).resolve()

EXPECTED_FILES = [
    "operational/customers.json",
    "operational/customer_profiles.json",
    "operational/customer_addresses.json",
    "operational/products.json",
    "operational/product_categories.json",
    "operational/stores.json",
    "operational/sales_channels.json",
    "operational/promotions.json",
    "operational/orders.json",
    "operational/order_items.json",
    "operational/order_promotions.json",

    "events/payment_events.json",
    "events/refund_events.json",
    "events/return_events.json",
    "events/support_events.json",
    "events/web_events.json",

    "inventory/inventory_snapshots.csv",

    "reference/campaign_spend.csv",
    "reference/city_reference.json",
]


# ============================================================
# 2. DATABASE SETUP
# ============================================================

def create_database():
    """
    Membuat database milestone1_omnichannel jika belum tersedia.
    """

    server_engine = create_engine(
        POSTGRES_SERVER_URL,
        isolation_level="AUTOCOMMIT",
    )

    with server_engine.connect() as connection:

        result = connection.execute(
            text("""
                SELECT 1
                FROM pg_database
                WHERE datname = :db_name
            """),
            {"db_name": DB_NAME},
        )

        database_exists = result.fetchone()

        if database_exists:
            print(f"Database '{DB_NAME}' sudah ada.")

        else:
            connection.execute(
                text(f'CREATE DATABASE "{DB_NAME}"')
            )

            print(f"Database '{DB_NAME}' berhasil dibuat.")

    server_engine.dispose()


def get_engine():
    """
    Membuat koneksi SQLAlchemy ke database milestone.
    """

    engine = create_engine(DATABASE_URL)

    with engine.connect() as connection:
        database = connection.execute(
            text("SELECT current_database();")
        ).scalar()

    print("Connected to:", database)

    return engine


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def load_json(path):
    """
    Membaca source JSON.
    """

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return [data]

    raise ValueError(
        f"Format JSON tidak didukung: {path}"
    )


def load_csv(path):
    """
    Membaca source CSV.
    """

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:

        return list(csv.DictReader(file))


def create_checksum(record):
    """
    Membuat SHA256 checksum dari raw record.
    """

    payload = json.dumps(
        record,
        sort_keys=True,
        ensure_ascii=False,
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def get_source_record_id(record):
    """
    Mencari business/source identifier yang tersedia
    pada raw record.
    """

    if not isinstance(record, dict):
        return None

    preferred_keys = (
        "source_row_id",
        "event_id",
        "order_id",
        "order_item_id",
        "customer_id",
        "product_id",
        "promotion_id",
        "store_id",
        "channel_id",
        "category_id",
        "campaign_id",
        "address_id",
        "profile_id",
        "payment_id",
        "refund_id",
        "return_id",
        "support_id",
        "session_id",
    )

    for key in preferred_keys:

        value = record.get(key)

        if value is not None and value != "":
            return str(value)

    return None


def normalize_path(path):
    """
    Normalisasi path untuk proses validasi file.
    """

    return path.as_posix().lower()


# ============================================================
# 4. VALIDATE RAW FILES
# ============================================================

def validate_raw_files():
    """
    Memastikan seluruh raw source yang diwajibkan tersedia.
    """

    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw directory tidak ditemukan: {RAW_DIR}"
        )

    actual_files = [
        path
        for path in RAW_DIR.rglob("*")
        if path.is_file()
    ]

    missing_files = []
    matched_files = {}

    for expected in EXPECTED_FILES:

        expected_normalized = expected.lower()

        matches = [
            path
            for path in actual_files
            if normalize_path(path).endswith(
                expected_normalized
            )
        ]

        if not matches:
            missing_files.append(expected)

        else:
            matched_files[expected] = matches[0]

    if missing_files:

        raise FileNotFoundError(
            "Raw files berikut tidak ditemukan:\n"
            + "\n".join(missing_files)
        )

    print(
        f"Validasi berhasil: "
        f"{len(matched_files)}/{len(EXPECTED_FILES)} "
        f"raw files ditemukan."
    )

    for expected in EXPECTED_FILES:
        print(f"[OK] {expected}")

    return matched_files


# ============================================================
# 5. BUILD BRONZE RECORDS
# ============================================================

def build_bronze_records(matched_files):
    """
    Membentuk raw records untuk Bronze.

    Tidak melakukan business transformation.
    Payload asli tetap disimpan sebagai JSONB.
    """

    pipeline_run_id = os.getenv(
    "PIPELINE_RUN_ID",
    str(uuid.uuid4())
)
    ingested_at = datetime.now(timezone.utc)

    bronze_rows = []

    for expected_file in EXPECTED_FILES:

        path = matched_files[expected_file]

        if path.suffix.lower() == ".json":
            records = load_json(path)

        elif path.suffix.lower() == ".csv":
            records = load_csv(path)

        else:
            continue

        for row_number, record in enumerate(
            records,
            start=1,
        ):

            bronze_rows.append({
                "raw_record_id": str(uuid.uuid4()),
                "pipeline_run_id": pipeline_run_id,
                "ingested_at_utc": ingested_at,
                "source_file": expected_file,
                "source_row_number": row_number,
                "source_record_id":
                    get_source_record_id(record),
                "record_checksum":
                    create_checksum(record),
                "raw_payload": record,
            })

    bronze_df = pd.DataFrame(bronze_rows)

    print()
    print("Total Bronze records:", len(bronze_df))
    print("Pipeline run ID     :", pipeline_run_id)

    return bronze_df, pipeline_run_id


# ============================================================
# 6. CREATE BRONZE SCHEMA
# ============================================================

def create_bronze_schema(engine):
    """
    Membuat schema dan tabel Bronze jika belum tersedia.
    """

    with engine.begin() as connection:

        connection.execute(
            text("""
                CREATE SCHEMA IF NOT EXISTS bronze;
            """)
        )

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS bronze.raw_records (
                    raw_record_id UUID PRIMARY KEY,
                    pipeline_run_id UUID NOT NULL,
                    ingested_at_utc TIMESTAMPTZ NOT NULL,
                    source_file TEXT NOT NULL,
                    source_row_number INTEGER NOT NULL,
                    source_record_id TEXT,
                    record_checksum TEXT NOT NULL,
                    raw_payload JSONB NOT NULL
                );
            """)
        )

    print("Schema dan tabel Bronze siap.")


# ============================================================
# 7. FILTER RECORDS FOR SAFE RERUN
# ============================================================

def filter_new_records(engine, bronze_df):
    """
    Mencegah record yang sama dimasukkan ulang ketika
    pipeline dijalankan kembali.

    Identitas source menggunakan:
        source_file
        + source_row_number
        + record_checksum

    Checksum saja tidak digunakan karena duplicate business
    records dari source tetap harus dipertahankan di Bronze.
    """

    existing_records = pd.read_sql(
        text("""
            SELECT
                source_file,
                source_row_number,
                record_checksum
            FROM bronze.raw_records
        """),
        engine,
    )

    if existing_records.empty:
        return bronze_df.copy()

    existing_keys = set(
        zip(
            existing_records["source_file"],
            existing_records["source_row_number"],
            existing_records["record_checksum"],
        )
    )

    new_mask = [
        (
            row.source_file,
            row.source_row_number,
            row.record_checksum,
        ) not in existing_keys
        for row in bronze_df.itertuples()
    ]

    bronze_new = bronze_df.loc[new_mask].copy()

    print()
    print("Total data :", len(bronze_df))
    print(
        "Sudah ada  :",
        len(bronze_df) - len(bronze_new),
    )
    print("Data baru  :", len(bronze_new))

    return bronze_new


# ============================================================
# 8. INSERT BRONZE
# ============================================================

def insert_bronze(engine, bronze_new):
    """
    Menyimpan raw records ke PostgreSQL Bronze.
    """

    if bronze_new.empty:
        print(
            "Tidak ada record baru yang perlu dimasukkan."
        )
        return

    bronze_new.to_sql(
        name="raw_records",
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False,
        dtype={
            "raw_payload": JSONB,
        },
    )

    print(
        f"Inserted {len(bronze_new)} Bronze records."
    )


# ============================================================
# 9. VALIDATE PIPELINE RUN
# ============================================================

def validate_pipeline_run(engine, pipeline_run_id):
    """
    Melihat hasil ingestion untuk pipeline run saat ini.
    """

    stored_bronze = pd.read_sql(
        text("""
            SELECT
                raw_record_id,
                pipeline_run_id,
                ingested_at_utc,
                source_file,
                source_row_number,
                source_record_id,
                record_checksum,
                raw_payload
            FROM bronze.raw_records
            WHERE pipeline_run_id = :pipeline_run_id
        """),
        engine,
        params={
            "pipeline_run_id": pipeline_run_id,
        },
    )

    print()
    print("=" * 60)
    print("PIPELINE RUN SUMMARY")
    print("=" * 60)

    print(
        "Stored records:",
        len(stored_bronze),
    )

    if not stored_bronze.empty:

        source_counts = (
            stored_bronze
            .groupby("source_file")
            .size()
            .sort_index()
        )

        print()
        print(source_counts)

    return stored_bronze


# ============================================================
# 10. BRONZE QUALITY CHECK
# ============================================================

def run_quality_check(engine):
    """
    Menjalankan final Bronze Quality Gate.
    """

    bronze = pd.read_sql(
        text("""
            SELECT *
            FROM bronze.raw_records
        """),
        engine,
    )

    print()
    print(
        "Bronze berhasil dimuat:",
        len(bronze),
        "rows",
    )

    quality_results = []

    def add_check(
        check_name,
        actual,
        expected,
        passed,
    ):

        quality_results.append({
            "check_name": check_name,
            "actual": actual,
            "expected": expected,
            "status":
                "PASS" if passed else "FAIL",
        })

    # --------------------------------------------------------
    # Bronze tidak boleh kosong
    # --------------------------------------------------------

    bronze_count = len(bronze)

    add_check(
        "Bronze contains records",
        bronze_count,
        "> 0",
        bronze_count > 0,
    )

    # --------------------------------------------------------
    # Mandatory fields
    # --------------------------------------------------------

    null_payload = (
        bronze["raw_payload"].isna().sum()
    )

    add_check(
        "Null raw_payload",
        null_payload,
        0,
        null_payload == 0,
    )

    null_source_file = (
        bronze["source_file"].isna().sum()
    )

    add_check(
        "Null source_file",
        null_source_file,
        0,
        null_source_file == 0,
    )

    null_source_row = (
        bronze["source_row_number"].isna().sum()
    )

    add_check(
        "Null source_row_number",
        null_source_row,
        0,
        null_source_row == 0,
    )

    null_checksum = (
        bronze["record_checksum"].isna().sum()
    )

    add_check(
        "Null record_checksum",
        null_checksum,
        0,
        null_checksum == 0,
    )

    null_pipeline_run = (
        bronze["pipeline_run_id"].isna().sum()
    )

    add_check(
        "Null pipeline_run_id",
        null_pipeline_run,
        0,
        null_pipeline_run == 0,
    )

    # --------------------------------------------------------
    # raw_record_id harus unik
    # --------------------------------------------------------

    duplicate_raw_id = (
        bronze["raw_record_id"]
        .duplicated()
        .sum()
    )

    add_check(
        "Duplicate raw_record_id",
        duplicate_raw_id,
        0,
        duplicate_raw_id == 0,
    )

    # --------------------------------------------------------
    # Quality Report
    # --------------------------------------------------------

    quality_report = pd.DataFrame(
        quality_results
    )

    print()
    print("=" * 60)
    print("BRONZE QUALITY REPORT")
    print("=" * 60)

    print(
        quality_report.to_string(index=False)
    )

    failed_checks = quality_report[
        quality_report["status"] == "FAIL"
    ]

    # --------------------------------------------------------
    # Final Quality Gate
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("FINAL BRONZE QUALITY GATE")
    print("=" * 60)

    print(
        "Total Bronze rows :",
        bronze_count,
    )
    print(
        "Null raw_payload  :",
        null_payload,
    )
    print(
        "Null source_file  :",
        null_source_file,
    )
    print(
        "Null source row   :",
        null_source_row,
    )
    print(
        "Null checksum     :",
        null_checksum,
    )
    print(
        "Null pipeline run :",
        null_pipeline_run,
    )
    print(
        "Duplicate raw ID  :",
        duplicate_raw_id,
    )

    print("=" * 60)

    if failed_checks.empty:

        print("BRONZE QUALITY GATE: PASS")
        print(
            "Bronze layer siap digunakan oleh Silver."
        )

    else:

        print("BRONZE QUALITY GATE: FAIL")
        print(
            "Jumlah check gagal:",
            len(failed_checks),
        )

        print(
            failed_checks.to_string(index=False)
        )

        raise RuntimeError(
            "Bronze Quality Gate gagal."
        )

    return quality_report


# ============================================================
# 11. MAIN BRONZE PIPELINE
# ============================================================

def main():

    print("=" * 60)
    print("START BRONZE PIPELINE")
    print("=" * 60)

    # 1. Buat database jika belum tersedia
    create_database()

    # 2. Connect ke database
    engine = get_engine()

    try:

        # 3. Validasi raw files
        matched_files = validate_raw_files()

        # 4. Membentuk Bronze records
        bronze_df, pipeline_run_id = (
            build_bronze_records(
                matched_files
            )
        )

        # 5. Siapkan schema Bronze
        create_bronze_schema(engine)

        # 6. Safe rerun protection
        bronze_new = filter_new_records(
            engine,
            bronze_df,
        )

        # 7. Insert ke Bronze
        insert_bronze(
            engine,
            bronze_new,
        )

        # 8. Validasi ingestion run
        validate_pipeline_run(
            engine,
            pipeline_run_id,
        )

        # 9. Final Quality Gate
        run_quality_check(engine)

        print()
        print("=" * 60)
        print("BRONZE PIPELINE SELESAI")
        print("=" * 60)

    finally:

        engine.dispose()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()