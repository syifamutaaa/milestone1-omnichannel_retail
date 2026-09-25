# ============================================================
# SILVER LAYER — OMNICHANNEL RETAIL
# ============================================================
# Silver membaca bronze.raw_records lalu membangun layer Silver:
# - typed dan normalized
# - timestamp dinormalisasi ke UTC
# - deterministic deduplication
# - foreign reference validation
# - status/event type normalization
# - rejected records
# - event identity resolution
# - temporal version handling
# - lineage ke Bronze
# - late-arriving detection
#
# Silver TIDAK membaca raw file secara langsung.
# ============================================================


# ============================================================
# 1. IMPORT DAN KONEKSI POSTGRESQL
# ============================================================

import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
pipeline_run_id = os.getenv(
    "PIPELINE_RUN_ID"
)

if not pipeline_run_id:
    raise RuntimeError(
        "PIPELINE_RUN_ID tidak diterima dari Airflow."
    )

repo_root = Path.cwd()

if not (repo_root / ".env").exists():
    repo_root = repo_root.parent

load_dotenv(repo_root / ".env")


# Mengikuti database Bronze yang sebelumnya dipakai.
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



engine = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
)


with engine.connect() as connection:
    connection.execute(text("SELECT 1"))


print("Database connection berhasil.")


# ============================================================
# 2. BACA BRONZE
# ============================================================

bronze = pd.read_sql(
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
        ORDER BY source_file, source_row_number
    """),
    engine,
)


if bronze.empty:
    raise RuntimeError(
        "Bronze kosong. Jalankan Bronze terlebih dahulu."
    )


print("Total Bronze records:", len(bronze))
print("Jumlah source file:", bronze["source_file"].nunique())
print(bronze.head())


# ============================================================
# 3. HELPER FUNCTION
# ============================================================

def payload_as_dict(value):

    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        return json.loads(value)

    raise ValueError(
        "raw_payload bukan object JSON yang valid"
    )


def rows_from_source(filename):
    """
    Ambil Bronze berdasarkan nama file,
    tanpa bergantung prefix folder.
    """

    source_path = bronze["source_file"].str.replace(
        "\\",
        "/",
        regex=False
    )

    mask = (
        source_path.str.endswith("/" + filename)
        | (source_path == filename)
    )

    part = bronze.loc[mask].copy()

    if part.empty:

        print(
            f"[WARNING] Source tidak ditemukan: {filename}"
        )

        return pd.DataFrame()

    # Buka raw_payload
    payload = part["raw_payload"].apply(
        payload_as_dict
    )

    normalized = pd.json_normalize(payload)

    # Flatten nested payload pada event
    normalized.columns = [
        col.replace("payload.", "")
        if col.startswith("payload.")
        else col
        for col in normalized.columns
    ]

    # Metadata lineage dari Bronze
    lineage = part[
        [
            "raw_record_id",
            "pipeline_run_id",
            "ingested_at_utc",
            "source_file",
            "source_row_number",
            "record_checksum",
        ]
    ].copy()

    lineage = lineage.rename(
        columns={
            "ingested_at_utc":
                "bronze_ingested_at_utc"
        }
    )

    return pd.concat(
        [
            normalized.reset_index(drop=True),
            lineage.reset_index(drop=True)
        ],
        axis=1
    )


def to_utc(series):

    return pd.to_datetime(
        series,
        errors="coerce",
        utc=True
    )


def to_date(series):

    return pd.to_datetime(
        series,
        errors="coerce"
    ).dt.date


def to_number(series):

    return pd.to_numeric(
        series,
        errors="coerce"
    )


def clean_text(series):

    return (
        series
        .astype("string")
        .str.strip()
    )


def normalize_upper(series):

    return clean_text(series).str.upper()


def deterministic_dedup(
    df,
    key,
    time_col=None
):
    """
    Menentukan satu record pemenang
    secara deterministic.
    """

    if df.empty:
        return df.copy(), df.copy()

    work = df.copy()

    sort_cols = [key]
    ascending = [True]

    if time_col and time_col in work.columns:

        sort_cols.append(time_col)
        ascending.append(False)

    sort_cols += [
        "bronze_ingested_at_utc",
        "source_file",
        "source_row_number",
        "raw_record_id"
    ]

    ascending += [
        False,
        True,
        False,
        False
    ]

    work = work.sort_values(
        sort_cols,
        ascending=ascending,
        na_position="last"
    )

    duplicate_mask = work.duplicated(
        subset=[key],
        keep="first"
    )

    return (
        work.loc[
            ~duplicate_mask
        ].copy(),

        work.loc[
            duplicate_mask
        ].copy()
    )


rejected_parts = []


def reject_rows(
    df,
    mask,
    reason,
    source_entity
):

    if df.empty or not mask.any():
        return df

    bad = df.loc[mask].copy()

    rejected_parts.append(
        pd.DataFrame({
            "source_entity":
                source_entity,

            "source_record_id":
                bad.get(
                    "source_row_id",
                    bad.get(
                        "event_id",
                        pd.Series(
                            index=bad.index,
                            dtype="object"
                        )
                    )
                ),

            "rejection_reason":
                reason,

            "raw_record_id":
                bad["raw_record_id"],

            "pipeline_run_id":
                bad["pipeline_run_id"],

            "source_file":
                bad["source_file"],

            "source_row_number":
                bad["source_row_number"],

            "record_checksum":
                bad["record_checksum"],
        })
    )

    return df.loc[~mask].copy()


def reject_duplicates(
    duplicates,
    key,
    source_entity
):

    if duplicates.empty:
        return

    rejected_parts.append(
        pd.DataFrame({
            "source_entity":
                source_entity,

            "source_record_id":
                duplicates[key].astype("string"),

            "rejection_reason":
                "duplicate_" + key,

            "raw_record_id":
                duplicates["raw_record_id"],

            "pipeline_run_id":
                duplicates["pipeline_run_id"],

            "source_file":
                duplicates["source_file"],

            "source_row_number":
                duplicates["source_row_number"],

            "record_checksum":
                duplicates["record_checksum"],
        })
    )


def add_lineage_columns(df):

    keep = [
        "raw_record_id",
        "pipeline_run_id",
        "source_file",
        "source_row_number",
        "record_checksum"
    ]

    return [
        c
        for c in keep
        if c in df.columns
    ]


def late_arriving_flag(df):

    if (
        "occurred_at_utc" in df.columns
        and "ingested_at_utc" in df.columns
    ):

        return (
            df["ingested_at_utc"]
            > df["occurred_at_utc"]
        )

    return False


# ============================================================
# 4. MASTER / REFERENCE TABLES
# ============================================================


# ------------------------------------------------------------
# CITY
# ------------------------------------------------------------

cities = rows_from_source(
    "city_reference.json"
)

cities["city_id"] = clean_text(
    cities["city_id"]
)

cities["city_name"] = clean_text(
    cities["city_name"]
)

cities["country"] = normalize_upper(
    cities["country"]
)

cities, dup = deterministic_dedup(
    cities,
    "city_id"
)

reject_duplicates(
    dup,
    "city_id",
    "cities"
)

cities = reject_rows(
    cities,
    cities["city_id"].isna()
    | cities["city_name"].isna(),
    "missing_required_field",
    "cities"
)


# ------------------------------------------------------------
# SALES CHANNEL
# ------------------------------------------------------------

channels = rows_from_source(
    "sales_channels.json"
)

channels["channel_id"] = normalize_upper(
    channels["channel_id"]
)

channels["channel_name"] = normalize_upper(
    channels["channel_name"]
)

channels, dup = deterministic_dedup(
    channels,
    "channel_id"
)

reject_duplicates(
    dup,
    "channel_id",
    "sales_channels"
)

channels = reject_rows(
    channels,
    channels["channel_id"].isna(),
    "missing_channel_id",
    "sales_channels"
)


# ------------------------------------------------------------
# PRODUCTS
# ------------------------------------------------------------

products = rows_from_source(
    "products.json"
)

products["product_id"] = clean_text(
    products["product_id"]
)

products["product_name"] = clean_text(
    products["product_name"]
)

products["sku"] = clean_text(
    products["sku"]
)

products["unit_price"] = to_number(
    products["unit_price"]
)

products, dup = deterministic_dedup(
    products,
    "product_id"
)

reject_duplicates(
    dup,
    "product_id",
    "products"
)

products = reject_rows(
    products,

    products["product_id"].isna()
    | products["unit_price"].isna()
    | (products["unit_price"] < 0),

    "invalid_product",
    "products",
)


# ------------------------------------------------------------
# STORES
# ------------------------------------------------------------

stores = rows_from_source(
    "stores.json"
)

stores["store_id"] = clean_text(
    stores["store_id"]
)

stores["city_id"] = clean_text(
    stores["city_id"]
)

stores["location_type"] = (
    clean_text(
        stores["location_type"]
    )
    .str.lower()
)

stores, dup = deterministic_dedup(
    stores,
    "store_id"
)

reject_duplicates(
    dup,
    "store_id",
    "stores"
)

stores = reject_rows(
    stores,
    stores["store_id"].isna(),
    "missing_store_id",
    "stores"
)


valid_city = set(
    cities["city_id"].dropna()
)


stores = reject_rows(
    stores,
    ~stores["city_id"].isin(valid_city),
    "invalid_city_reference",
    "stores"
)


# ------------------------------------------------------------
# PROMOTIONS
# ------------------------------------------------------------

promotions = rows_from_source(
    "promotions.json"
)

promotions["promotion_id"] = clean_text(
    promotions["promotion_id"]
)

promotions["promotion_code"] = normalize_upper(
    promotions["promotion_code"]
)

promotions["promotion_type"] = (
    clean_text(
        promotions["promotion_type"]
    )
    .str.lower()
)

promotions["discount_rate"] = to_number(
    promotions["discount_rate"]
)

promotions["start_date"] = to_date(
    promotions["start_date"]
)

promotions["end_date"] = to_date(
    promotions["end_date"]
)

promotions, dup = deterministic_dedup(
    promotions,
    "promotion_id"
)

reject_duplicates(
    dup,
    "promotion_id",
    "promotions"
)

promotions = reject_rows(
    promotions,

    promotions["promotion_id"].isna()
    | promotions["start_date"].isna()
    | promotions["end_date"].isna()
    | (
        promotions["end_date"]
        < promotions["start_date"]
    ),

    "invalid_promotion",
    "promotions",
)


print("cities:", len(cities))
print("channels:", len(channels))
print("products:", len(products))
print("stores:", len(stores))
print("promotions:", len(promotions))


# ============================================================
# 5. CUSTOMER + TEMPORAL VERSION SELECTION
# ============================================================


# ------------------------------------------------------------
# CUSTOMERS
# ------------------------------------------------------------

customers = rows_from_source(
    "customers.json"
)

customers["customer_id"] = clean_text(
    customers["customer_id"]
)

customers["city_id"] = clean_text(
    customers["city_id"]
)

customers["customer_segment"] = (
    clean_text(
        customers["customer_segment"]
    )
    .str.lower()
)

customers["created_at_utc"] = to_utc(
    customers["created_at_utc"]
)

customers, dup = deterministic_dedup(
    customers,
    "customer_id",
    "created_at_utc"
)

reject_duplicates(
    dup,
    "customer_id",
    "customers"
)

customers = reject_rows(
    customers,

    customers["customer_id"].isna()
    | customers["created_at_utc"].isna(),

    "invalid_customer",
    "customers",
)

customers = reject_rows(
    customers,
    ~customers["city_id"].isin(valid_city),
    "invalid_city_reference",
    "customers"
)


valid_customer = set(
    customers["customer_id"].dropna()
)


# ------------------------------------------------------------
# CUSTOMER PROFILES
# ------------------------------------------------------------

customer_profiles = rows_from_source(
    "customer_profiles.json"
)

customer_profiles["customer_id"] = clean_text(
    customer_profiles["customer_id"]
)

customer_profiles["city_id"] = clean_text(
    customer_profiles["city_id"]
)

customer_profiles["customer_segment"] = (
    clean_text(
        customer_profiles["customer_segment"]
    )
    .str.lower()
)

customer_profiles["valid_from_utc"] = to_utc(
    customer_profiles["valid_from_utc"]
)

customer_profiles["valid_to_utc"] = to_utc(
    customer_profiles["valid_to_utc"]
)


customer_profiles = reject_rows(
    customer_profiles,

    customer_profiles["customer_id"].isna()
    | customer_profiles["valid_from_utc"].isna()
    | customer_profiles["valid_to_utc"].isna()
    | (
        customer_profiles["valid_to_utc"]
        < customer_profiles["valid_from_utc"]
    ),

    "invalid_temporal_range",
    "customer_profiles",
)


customer_profiles = reject_rows(
    customer_profiles,

    ~customer_profiles[
        "customer_id"
    ].isin(valid_customer),

    "invalid_customer_reference",
    "customer_profiles",
)


customer_profiles = reject_rows(
    customer_profiles,

    ~customer_profiles[
        "city_id"
    ].isin(valid_city),

    "invalid_city_reference",
    "customer_profiles",
)


customer_profiles["version_key"] = (
    customer_profiles[
        "customer_id"
    ].astype(str)
    + "|"
    + customer_profiles[
        "valid_from_utc"
    ].astype(str)
)


customer_profiles, dup = deterministic_dedup(
    customer_profiles,
    "version_key",
    "valid_from_utc"
)

reject_duplicates(
    dup,
    "version_key",
    "customer_profiles"
)


customer_profiles["is_current"] = (
    customer_profiles["valid_from_utc"]
    == customer_profiles.groupby(
        "customer_id"
    )["valid_from_utc"].transform("max")
)


# ------------------------------------------------------------
# CUSTOMER ADDRESSES
# ------------------------------------------------------------

customer_addresses = rows_from_source(
    "customer_addresses.json"
)

customer_addresses["address_id"] = clean_text(
    customer_addresses["address_id"]
)

customer_addresses["customer_id"] = clean_text(
    customer_addresses["customer_id"]
)

customer_addresses["city_id"] = clean_text(
    customer_addresses["city_id"]
)

customer_addresses["valid_from_utc"] = to_utc(
    customer_addresses["valid_from_utc"]
)

customer_addresses["valid_to_utc"] = to_utc(
    customer_addresses["valid_to_utc"]
)


customer_addresses = reject_rows(
    customer_addresses,

    customer_addresses["address_id"].isna()
    | customer_addresses[
        "valid_from_utc"
    ].isna()
    | customer_addresses[
        "valid_to_utc"
    ].isna()
    | (
        customer_addresses["valid_to_utc"]
        < customer_addresses["valid_from_utc"]
    ),

    "invalid_address",
    "customer_addresses",
)


customer_addresses = reject_rows(
    customer_addresses,

    ~customer_addresses[
        "customer_id"
    ].isin(valid_customer),

    "invalid_customer_reference",
    "customer_addresses",
)


customer_addresses = reject_rows(
    customer_addresses,

    ~customer_addresses[
        "city_id"
    ].isin(valid_city),

    "invalid_city_reference",
    "customer_addresses",
)


customer_addresses, dup = deterministic_dedup(
    customer_addresses,
    "address_id",
    "valid_from_utc"
)

reject_duplicates(
    dup,
    "address_id",
    "customer_addresses"
)


customer_addresses["is_current"] = (
    customer_addresses["valid_from_utc"]
    == customer_addresses.groupby(
        "customer_id"
    )["valid_from_utc"].transform("max")
)


print("customers:", len(customers))
print(
    "customer_profiles:",
    len(customer_profiles)
)
print(
    "customer_addresses:",
    len(customer_addresses)
)


# ============================================================
# 6. PRODUCT CATEGORY TEMPORAL VERSIONS
# ============================================================

product_categories = rows_from_source(
    "product_categories.json"
)

product_categories["product_id"] = clean_text(
    product_categories["product_id"]
)

product_categories["category_id"] = clean_text(
    product_categories["category_id"]
)

product_categories["category_name"] = (
    clean_text(
        product_categories["category_name"]
    )
    .str.lower()
)

product_categories["valid_from_utc"] = to_utc(
    product_categories["valid_from_utc"]
)

product_categories["valid_to_utc"] = to_utc(
    product_categories["valid_to_utc"]
)


valid_product = set(
    products["product_id"].dropna()
)


product_categories = reject_rows(
    product_categories,

    product_categories["product_id"].isna()
    | product_categories["category_id"].isna()
    | product_categories[
        "valid_from_utc"
    ].isna()
    | product_categories[
        "valid_to_utc"
    ].isna()
    | (
        product_categories["valid_to_utc"]
        < product_categories["valid_from_utc"]
    ),

    "invalid_product_category",
    "product_categories",
)


product_categories = reject_rows(
    product_categories,

    ~product_categories[
        "product_id"
    ].isin(valid_product),

    "invalid_product_reference",
    "product_categories",
)


product_categories["version_key"] = (
    product_categories[
        "product_id"
    ].astype(str)
    + "|"
    + product_categories[
        "valid_from_utc"
    ].astype(str)
)


product_categories, dup = deterministic_dedup(
    product_categories,
    "version_key",
    "valid_from_utc"
)

reject_duplicates(
    dup,
    "version_key",
    "product_categories"
)


product_categories["is_current"] = (
    product_categories["valid_from_utc"]
    == product_categories.groupby(
        "product_id"
    )["valid_from_utc"].transform("max")
)


print(
    "product_categories:",
    len(product_categories)
)


# ============================================================
# 7. ORDERS DAN ORDER ITEMS
# ============================================================


# ------------------------------------------------------------
# ORDERS
# ------------------------------------------------------------

orders = rows_from_source(
    "orders.json"
)

orders["order_id"] = clean_text(
    orders["order_id"]
)

orders["customer_id"] = clean_text(
    orders["customer_id"]
)

orders["sales_channel"] = normalize_upper(
    orders["sales_channel"]
)

orders["store_id"] = clean_text(
    orders["store_id"]
)

orders["status"] = normalize_upper(
    orders["status"]
)

orders["shipping_revenue"] = to_number(
    orders["shipping_revenue"]
)

orders["ordered_at_utc"] = to_utc(
    orders["ordered_at_utc"]
)

orders["updated_at_utc"] = to_utc(
    orders["updated_at_utc"]
)


# Duplicate order_id:
# pilih updated_at terbaru secara deterministic.
orders, dup = deterministic_dedup(
    orders,
    "order_id",
    "updated_at_utc"
)

reject_duplicates(
    dup,
    "order_id",
    "orders"
)


orders = reject_rows(
    orders,

    orders["order_id"].isna()
    | orders["customer_id"].isna()
    | orders["ordered_at_utc"].isna()
    | orders["updated_at_utc"].isna()
    | orders["shipping_revenue"].isna()
    | (orders["shipping_revenue"] < 0),

    "invalid_order",
    "orders",
)


orders = reject_rows(
    orders,

    ~orders[
        "customer_id"
    ].isin(valid_customer),

    "invalid_customer_reference",
    "orders"
)


valid_channels = set(
    channels["channel_id"].dropna()
)


orders = reject_rows(
    orders,

    ~orders[
        "sales_channel"
    ].isin(valid_channels),

    "invalid_sales_channel_reference",
    "orders"
)


valid_store = set(
    stores["store_id"].dropna()
)


bad_store = (
    orders["store_id"].notna()
    & ~orders["store_id"].isin(valid_store)
)


orders = reject_rows(
    orders,
    bad_store,
    "invalid_store_reference",
    "orders"
)


valid_order = set(
    orders["order_id"].dropna()
)


# ------------------------------------------------------------
# ORDER ITEMS
# ------------------------------------------------------------

order_items = rows_from_source(
    "order_items.json"
)

order_items["order_item_id"] = clean_text(
    order_items["order_item_id"]
)

order_items["order_id"] = clean_text(
    order_items["order_id"]
)

order_items["product_id"] = clean_text(
    order_items["product_id"]
)

order_items["quantity"] = to_number(
    order_items["quantity"]
)

order_items["unit_price"] = to_number(
    order_items["unit_price"]
)

order_items["item_discount_amount"] = to_number(
    order_items["item_discount_amount"]
)


order_items, dup = deterministic_dedup(
    order_items,
    "order_item_id"
)

reject_duplicates(
    dup,
    "order_item_id",
    "order_items"
)


order_items = reject_rows(
    order_items,

    order_items["order_item_id"].isna()
    | order_items["quantity"].isna()
    | (order_items["quantity"] <= 0)
    | (order_items["quantity"] % 1 != 0),

    "quantity_must_be_positive_integer",
    "order_items",
)


order_items = reject_rows(
    order_items,

    order_items["unit_price"].isna()
    | (order_items["unit_price"] < 0)
    | order_items[
        "item_discount_amount"
    ].isna()
    | (
        order_items[
            "item_discount_amount"
        ] < 0
    ),

    "invalid_order_item_amount",
    "order_items",
)


order_items = reject_rows(
    order_items,

    ~order_items[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "order_items"
)


order_items = reject_rows(
    order_items,

    ~order_items[
        "product_id"
    ].isin(valid_product),

    "invalid_product_reference",
    "order_items"
)


order_items["quantity"] = (
    order_items["quantity"]
    .astype("int64")
)


print("orders:", len(orders))
print("order_items:", len(order_items))


# ============================================================
# 8. ORDER PROMOTIONS
# ============================================================

order_promotions = rows_from_source(
    "order_promotions.json"
)

order_promotions["order_id"] = clean_text(
    order_promotions["order_id"]
)

order_promotions["promotion_id"] = clean_text(
    order_promotions["promotion_id"]
)

order_promotions["discount_amount"] = to_number(
    order_promotions["discount_amount"]
)


order_promotions["bridge_key"] = (
    order_promotions[
        "order_id"
    ].astype(str)
    + "|"
    + order_promotions[
        "promotion_id"
    ].astype(str)
)


order_promotions, dup = deterministic_dedup(
    order_promotions,
    "bridge_key"
)

reject_duplicates(
    dup,
    "bridge_key",
    "order_promotions"
)


order_promotions = reject_rows(
    order_promotions,

    order_promotions[
        "discount_amount"
    ].isna()
    | (
        order_promotions[
            "discount_amount"
        ] < 0
    ),

    "invalid_discount_amount",
    "order_promotions",
)


order_promotions = reject_rows(
    order_promotions,

    ~order_promotions[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "order_promotions",
)


valid_promo = set(
    promotions["promotion_id"].dropna()
)


order_promotions = reject_rows(
    order_promotions,

    ~order_promotions[
        "promotion_id"
    ].isin(valid_promo),

    "invalid_promotion_reference",
    "order_promotions",
)


print(
    "order_promotions:",
    len(order_promotions)
)


# ============================================================
# 9. EVENT STREAMS
# ============================================================
#
# Semua event:
# 1. timestamp dikonversi ke UTC
# 2. event_id dipakai sebagai deterministic identity
# 3. duplicate event_id memilih ingested_at_utc terbaru
# 4. foreign reference divalidasi
# 5. is_late_arriving = ingested_at_utc > occurred_at_utc
# ============================================================


def prepare_event(
    filename,
    entity,
    allowed_types
):

    df = rows_from_source(filename)

    df["event_id"] = clean_text(
        df["event_id"]
    )

    df["event_type"] = normalize_upper(
        df["event_type"]
    )

    df["occurred_at_utc"] = to_utc(
        df["occurred_at_utc"]
    )

    df["ingested_at_utc"] = to_utc(
        df["ingested_at_utc"]
    )

    df = reject_rows(
        df,

        df["event_id"].isna()
        | df["occurred_at_utc"].isna()
        | df["ingested_at_utc"].isna()
        | ~df[
            "event_type"
        ].isin(allowed_types),

        "invalid_event",
        entity,
    )

    # Event identity resolution
    df, dup = deterministic_dedup(
        df,
        "event_id",
        "ingested_at_utc"
    )

    reject_duplicates(
        dup,
        "event_id",
        entity
    )

    df["is_late_arriving"] = (
        late_arriving_flag(df)
    )

    df["arrival_delay_seconds"] = (
        df["ingested_at_utc"]
        - df["occurred_at_utc"]
    ).dt.total_seconds()

    return df


# ------------------------------------------------------------
# PAYMENT EVENTS
# ------------------------------------------------------------

payment_events = prepare_event(
    "payment_events.json",
    "payment_events",
    {
        "PAYMENT_AUTHORIZED",
        "PAYMENT_CAPTURED",
        "PAYMENT_FAILED"
    },
)


payment_events["order_id"] = clean_text(
    payment_events["order_id"]
)

payment_events["payment_id"] = clean_text(
    payment_events["payment_id"]
)

payment_events["amount"] = to_number(
    payment_events["amount"]
)


payment_events = reject_rows(
    payment_events,

    payment_events["payment_id"].isna()
    | payment_events["amount"].isna()
    | (payment_events["amount"] < 0),

    "invalid_payment_event",
    "payment_events",
)


payment_events = reject_rows(
    payment_events,

    ~payment_events[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "payment_events",
)


# ============================================================
# DETEKSI CANCELLED ORDER YANG PAYMENT-NYA SUDAH CAPTURED
# ============================================================
#
# Blok ini pada notebook menggunakan payment_events.
# Pada script .py diletakkan setelah payment_events dibentuk
# supaya dapat dijalankan top-to-bottom.
# Logikanya tetap sama.
# ============================================================


captured_orders = set(
    payment_events.loc[
        payment_events[
            "event_type"
        ].eq("PAYMENT_CAPTURED"),
        "order_id"
    ].dropna()
)


orders["has_captured_payment"] = (
    orders[
        "order_id"
    ].isin(captured_orders)
)


orders[
    "cancelled_with_captured_payment"
] = (
    orders["status"].eq("CANCELLED")
    & orders["has_captured_payment"]
)


cancelled_captured_orders = orders.loc[
    orders[
        "cancelled_with_captured_payment"
    ],
    [
        "order_id",
        "status",
        "ordered_at_utc",
        "updated_at_utc",
        "has_captured_payment",
        "cancelled_with_captured_payment"
    ]
].copy()


print(
    "Cancelled order dengan captured payment:",
    len(cancelled_captured_orders)
)

print(
    cancelled_captured_orders.head(20)
)


# ------------------------------------------------------------
# REFUND EVENTS
# ------------------------------------------------------------

refund_events = prepare_event(
    "refund_events.json",
    "refund_events",
    {
        "REFUND_ISSUED",
        "REFUND_COMPLETED"
    },
)


refund_events["order_id"] = clean_text(
    refund_events["order_id"]
)

refund_events["refund_id"] = clean_text(
    refund_events["refund_id"]
)

refund_events["amount"] = to_number(
    refund_events["amount"]
)


refund_events = reject_rows(
    refund_events,

    refund_events["refund_id"].isna()
    | refund_events["amount"].isna()
    | (refund_events["amount"] < 0),

    "invalid_refund_event",
    "refund_events",
)


refund_events = reject_rows(
    refund_events,

    ~refund_events[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "refund_events",
)


# ------------------------------------------------------------
# RETURN EVENTS
# ------------------------------------------------------------

return_events = prepare_event(
    "return_events.json",
    "return_events",
    {
        "RETURN_REQUESTED",
        "RETURN_RECEIVED",
        "RETURN_CLOSED"
    },
)


return_events["order_id"] = clean_text(
    return_events["order_id"]
)

return_events["return_id"] = clean_text(
    return_events["return_id"]
)


return_events = reject_rows(
    return_events,

    return_events[
        "return_id"
    ].isna(),

    "missing_return_id",
    "return_events",
)


return_events = reject_rows(
    return_events,

    ~return_events[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "return_events",
)


# ------------------------------------------------------------
# SUPPORT EVENTS
# ------------------------------------------------------------

support_events = prepare_event(
    "support_events.json",
    "support_events",
    {
        "SUPPORT_TICKET_CREATED",
        "SUPPORT_TICKET_CLOSED"
    },
)


support_events["customer_id"] = clean_text(
    support_events["customer_id"]
)

support_events["order_id"] = clean_text(
    support_events["order_id"]
)

support_events["ticket_id"] = clean_text(
    support_events["ticket_id"]
)

support_events["reason"] = (
    clean_text(
        support_events["reason"]
    )
    .str.lower()
)


support_events = reject_rows(
    support_events,

    support_events[
        "ticket_id"
    ].isna(),

    "missing_ticket_id",
    "support_events",
)


support_events = reject_rows(
    support_events,

    ~support_events[
        "customer_id"
    ].isin(valid_customer),

    "invalid_customer_reference",
    "support_events",
)


support_events = reject_rows(
    support_events,

    support_events["order_id"].notna()
    & ~support_events[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "support_events",
)


# ------------------------------------------------------------
# WEB EVENTS
# ------------------------------------------------------------

web_events = prepare_event(
    "web_events.json",
    "web_events",
    {
        "PAGE_VIEW",
        "ADD_TO_CART",
        "PURCHASE"
    },
)


web_events["customer_id"] = clean_text(
    web_events["customer_id"]
)

web_events["order_id"] = clean_text(
    web_events["order_id"]
)

web_events["campaign_id"] = clean_text(
    web_events["campaign_id"]
)

web_events["session_id"] = clean_text(
    web_events["session_id"]
)

web_events["channel"] = normalize_upper(
    web_events["channel"]
)


web_events = reject_rows(
    web_events,

    web_events[
        "session_id"
    ].isna(),

    "missing_session_id",
    "web_events",
)


web_events = reject_rows(
    web_events,

    web_events["customer_id"].notna()
    & ~web_events[
        "customer_id"
    ].isin(valid_customer),

    "invalid_customer_reference",
    "web_events",
)


web_events = reject_rows(
    web_events,

    web_events["order_id"].notna()
    & ~web_events[
        "order_id"
    ].isin(valid_order),

    "invalid_order_reference",
    "web_events",
)


web_events = reject_rows(
    web_events,

    ~web_events[
        "channel"
    ].isin(valid_channels),

    "invalid_channel_reference",
    "web_events",
)


print(
    "payment_events:",
    len(payment_events)
)

print(
    "refund_events:",
    len(refund_events)
)

print(
    "return_events:",
    len(return_events)
)

print(
    "support_events:",
    len(support_events)
)

print(
    "web_events:",
    len(web_events)
)


# ============================================================
# 10. EVENT STATE RESOLUTION
# ============================================================


def latest_state(
    df,
    identity_col,
    state_col="event_type"
):

    if df.empty:
        return pd.DataFrame()

    ordered = df.sort_values(
        [
            identity_col,
            "occurred_at_utc",
            "ingested_at_utc",
            "event_id"
        ],
        ascending=[
            True,
            True,
            True,
            True
        ],
    )

    latest = (
        ordered
        .groupby(
            identity_col,
            as_index=False
        )
        .tail(1)
        .copy()
    )

    latest = latest.rename(
        columns={
            state_col:
                "current_status",

            "occurred_at_utc":
                "status_occurred_at_utc",

            "event_id":
                "status_event_id",
        }
    )

    return latest


payment_state = latest_state(
    payment_events,
    "payment_id"
)

refund_state = latest_state(
    refund_events,
    "refund_id"
)

return_state = latest_state(
    return_events,
    "return_id"
)

support_state = latest_state(
    support_events,
    "ticket_id"
)


print(
    "payment_state:",
    len(payment_state)
)

print(
    "refund_state:",
    len(refund_state)
)

print(
    "return_state:",
    len(return_state)
)

print(
    "support_state:",
    len(support_state)
)


# ============================================================
# 11. INVENTORY SNAPSHOT DAN CAMPAIGN SPEND
# ============================================================


# ------------------------------------------------------------
# INVENTORY
# ------------------------------------------------------------

inventory = rows_from_source(
    "inventory_snapshots.csv"
)

inventory["product_id"] = clean_text(
    inventory["product_id"]
)

inventory["location_id"] = clean_text(
    inventory["location_id"]
)

inventory["snapshot_date"] = to_date(
    inventory["snapshot_date"]
)

inventory["available_quantity"] = to_number(
    inventory["available_quantity"]
)

inventory["reserved_quantity"] = to_number(
    inventory["reserved_quantity"]
)

inventory["unit_cost"] = to_number(
    inventory["unit_cost"]
)


inventory["inventory_key"] = (
    inventory[
        "product_id"
    ].astype(str)
    + "|"
    + inventory[
        "location_id"
    ].astype(str)
    + "|"
    + inventory[
        "snapshot_date"
    ].astype(str)
)


inventory, dup = deterministic_dedup(
    inventory,
    "inventory_key"
)

reject_duplicates(
    dup,
    "inventory_key",
    "inventory_snapshots"
)


inventory = reject_rows(
    inventory,

    inventory["snapshot_date"].isna()
    | inventory[
        "available_quantity"
    ].isna()
    | inventory[
        "reserved_quantity"
    ].isna()
    | inventory["unit_cost"].isna()
    | (
        inventory[
            "available_quantity"
        ] < 0
    )
    | (
        inventory[
            "reserved_quantity"
        ] < 0
    )
    | (
        inventory[
            "unit_cost"
        ] < 0
    ),

    "invalid_inventory_snapshot",
    "inventory_snapshots",
)


inventory = reject_rows(
    inventory,

    ~inventory[
        "product_id"
    ].isin(valid_product),

    "invalid_product_reference",
    "inventory_snapshots",
)


# location dapat berupa store atau channel/lokasi lain;
# validasi store dilakukan jika format STORE-*

bad_location = (
    inventory[
        "location_id"
    ].str.startswith(
        "STORE-",
        na=False
    )
    & ~inventory[
        "location_id"
    ].isin(valid_store)
)


inventory = reject_rows(
    inventory,
    bad_location,
    "invalid_location_reference",
    "inventory_snapshots",
)


# ------------------------------------------------------------
# CAMPAIGN SPEND
# ------------------------------------------------------------

campaign_spend = rows_from_source(
    "campaign_spend.csv"
)

campaign_spend["spend_date"] = to_date(
    campaign_spend["spend_date"]
)

campaign_spend["campaign_id"] = clean_text(
    campaign_spend["campaign_id"]
)

campaign_spend["channel"] = normalize_upper(
    campaign_spend["channel"]
)

campaign_spend["spend_amount"] = to_number(
    campaign_spend["spend_amount"]
)


campaign_spend["campaign_spend_key"] = (
    campaign_spend[
        "spend_date"
    ].astype(str)
    + "|"
    + campaign_spend[
        "campaign_id"
    ].astype(str)
    + "|"
    + campaign_spend[
        "channel"
    ].astype(str)
)


campaign_spend, dup = deterministic_dedup(
    campaign_spend,
    "campaign_spend_key"
)

reject_duplicates(
    dup,
    "campaign_spend_key",
    "campaign_spend"
)


campaign_spend = reject_rows(
    campaign_spend,

    campaign_spend[
        "spend_date"
    ].isna()
    | campaign_spend[
        "campaign_id"
    ].isna()
    | campaign_spend[
        "spend_amount"
    ].isna()
    | (
        campaign_spend[
            "spend_amount"
        ] < 0
    ),

    "invalid_campaign_spend",
    "campaign_spend",
)


campaign_spend = reject_rows(
    campaign_spend,

    ~campaign_spend[
        "channel"
    ].isin(valid_channels),

    "invalid_channel_reference",
    "campaign_spend",
)


print(
    "inventory_snapshots:",
    len(inventory)
)

print(
    "campaign_spend:",
    len(campaign_spend)
)


# ============================================================
# 12. CEK MISSING INVENTORY
# ============================================================
#
# Pada notebook bagian ini berada di bawah dan terdapat catatan:
# "Cek missing inventory dulu di paling bawah, Baru jalanin
# no 12 dan seterusnya".
#
# Di file .py blok ini harus dieksekusi sebelum silver_tables
# menggunakan missing_inventory_snapshots.
# LOGIKA TIDAK DIUBAH.
# ============================================================


inventory_check = inventory[
    [
        "product_id",
        "location_id",
        "snapshot_date"
    ]
].copy()


inventory_check["snapshot_date"] = (
    pd.to_datetime(
        inventory_check[
            "snapshot_date"
        ]
    )
    .dt.date
)


# Semua pasangan product-location
# yang memang ada di inventory

product_locations = inventory_check[
    [
        "product_id",
        "location_id"
    ]
].drop_duplicates()


# Rentang tanggal snapshot

min_date = pd.to_datetime(
    inventory_check[
        "snapshot_date"
    ]
).min()


max_date = pd.to_datetime(
    inventory_check[
        "snapshot_date"
    ]
).max()


expected_dates = pd.DataFrame({
    "snapshot_date":
        pd.date_range(
            min_date,
            max_date,
            freq="D"
        ).date
})


# Kombinasi yang seharusnya tersedia setiap hari

expected_inventory = (
    product_locations.merge(
        expected_dates,
        how="cross"
    )
)


# Snapshot yang benar-benar tersedia

actual_inventory = (
    inventory_check
    .drop_duplicates()
)


# Cari kombinasi yang hilang

missing_inventory_snapshots = (
    expected_inventory
    .merge(
        actual_inventory,
        on=[
            "product_id",
            "location_id",
            "snapshot_date"
        ],
        how="left",
        indicator=True
    )
)


missing_inventory_snapshots = (
    missing_inventory_snapshots[
        missing_inventory_snapshots[
            "_merge"
        ] == "left_only"
    ]
    .drop(
        columns="_merge"
    )
    .reset_index(
        drop=True
    )
)


print(
    "Tanggal inventory:",
    min_date,
    "s/d",
    max_date
)

print(
    "Actual inventory snapshots :",
    len(actual_inventory)
)

print(
    "Expected snapshots         :",
    len(expected_inventory)
)

print(
    "Missing inventory snapshots:",
    len(missing_inventory_snapshots)
)

print(
    missing_inventory_snapshots.head(20)
)


# ============================================================
# 13. GABUNGKAN REJECTED RECORDS
# ============================================================

if rejected_parts:

    rejected_records = pd.concat(
        rejected_parts,
        ignore_index=True
    )

    # Satu Bronze row cukup tercatat sekali
    # jika secara tidak sengaja masuk rejection
    # yang sama lebih dari sekali.

    rejected_records = (
        rejected_records
        .drop_duplicates(
            subset=[
                "raw_record_id",
                "source_entity",
                "rejection_reason"
            ]
        )
        .reset_index(drop=True)
    )

else:

    rejected_records = pd.DataFrame(
        columns=[
            "source_entity",
            "source_record_id",
            "rejection_reason",
            "raw_record_id",
            "pipeline_run_id",
            "source_file",
            "source_row_number",
            "record_checksum",
        ]
    )


rejected_records["rejected_at_utc"] = (
    pd.Timestamp.now(
        tz="UTC"
    )
)


print(
    "Total rejected records:",
    len(rejected_records)
)


if not rejected_records.empty:

    rejected_summary = (
        rejected_records
        .groupby(
            [
                "source_entity",
                "rejection_reason"
            ]
        )
        .size()
        .reset_index(
            name="row_count"
        )
        .sort_values(
            [
                "source_entity",
                "row_count"
            ],
            ascending=[
                True,
                False
            ]
        )
    )

    print(rejected_summary)

else:

    print(
        "Tidak ada rejected records."
    )


# ============================================================
# 14. PILIH KOLOM SILVER
# ============================================================

LINEAGE = [
    "raw_record_id",
    "pipeline_run_id",
    "source_file",
    "source_row_number",
    "record_checksum"
]


def select_existing(
    df,
    columns
):

    return df[
        [
            c
            for c in columns
            if c in df.columns
        ]
    ].copy()


silver_tables = {

    "cities":
        select_existing(
            cities,
            [
                "city_id",
                "city_name",
                "country"
            ] + LINEAGE
        ),

    "sales_channels":
        select_existing(
            channels,
            [
                "channel_id",
                "channel_name"
            ] + LINEAGE
        ),

    "products":
        select_existing(
            products,
            [
                "product_id",
                "product_name",
                "sku",
                "unit_price"
            ] + LINEAGE
        ),

    "stores":
        select_existing(
            stores,
            [
                "store_id",
                "store_name",
                "city_id",
                "location_type"
            ] + LINEAGE
        ),

    "promotions":
        select_existing(
            promotions,
            [
                "promotion_id",
                "promotion_code",
                "promotion_type",
                "discount_rate",
                "start_date",
                "end_date"
            ] + LINEAGE
        ),

    "customers":
        select_existing(
            customers,
            [
                "customer_id",
                "city_id",
                "customer_segment",
                "created_at_utc"
            ] + LINEAGE
        ),

    "customer_profiles":
        select_existing(
            customer_profiles,
            [
                "customer_id",
                "city_id",
                "customer_segment",
                "valid_from_utc",
                "valid_to_utc",
                "is_current"
            ] + LINEAGE
        ),

    "customer_addresses":
        select_existing(
            customer_addresses,
            [
                "address_id",
                "customer_id",
                "city_id",
                "valid_from_utc",
                "valid_to_utc",
                "is_current"
            ] + LINEAGE
        ),

    "product_categories":
        select_existing(
            product_categories,
            [
                "product_id",
                "category_id",
                "category_name",
                "valid_from_utc",
                "valid_to_utc",
                "is_current"
            ] + LINEAGE
        ),

    "orders":
        select_existing(
            orders,
            [
                "order_id",
                "customer_id",
                "ordered_at_utc",
                "sales_channel",
                "shipping_revenue",
                "status",
                "store_id",
                "updated_at_utc"
            ] + LINEAGE
        ),

    "order_items":
        select_existing(
            order_items,
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "item_discount_amount"
            ] + LINEAGE
        ),

    "order_promotions":
        select_existing(
            order_promotions,
            [
                "order_id",
                "promotion_id",
                "discount_amount"
            ] + LINEAGE
        ),

    "payment_events":
        select_existing(
            payment_events,
            [
                "event_id",
                "event_type",
                "occurred_at_utc",
                "ingested_at_utc",
                "payment_id",
                "order_id",
                "amount",
                "is_late_arriving",
                "arrival_delay_seconds"
            ] + LINEAGE
        ),

    "refund_events":
        select_existing(
            refund_events,
            [
                "event_id",
                "event_type",
                "occurred_at_utc",
                "ingested_at_utc",
                "refund_id",
                "order_id",
                "amount",
                "is_late_arriving",
                "arrival_delay_seconds"
            ] + LINEAGE
        ),

    "return_events":
        select_existing(
            return_events,
            [
                "event_id",
                "event_type",
                "occurred_at_utc",
                "ingested_at_utc",
                "return_id",
                "order_id",
                "is_late_arriving",
                "arrival_delay_seconds"
            ] + LINEAGE
        ),

    "support_events":
        select_existing(
            support_events,
            [
                "event_id",
                "event_type",
                "occurred_at_utc",
                "ingested_at_utc",
                "ticket_id",
                "customer_id",
                "order_id",
                "reason",
                "is_late_arriving",
                "arrival_delay_seconds"
            ] + LINEAGE
        ),

    "web_events":
        select_existing(
            web_events,
            [
                "event_id",
                "event_type",
                "occurred_at_utc",
                "ingested_at_utc",
                "session_id",
                "customer_id",
                "order_id",
                "campaign_id",
                "channel",
                "is_late_arriving",
                "arrival_delay_seconds"
            ] + LINEAGE
        ),

    "inventory_snapshots":
        select_existing(
            inventory,
            [
                "product_id",
                "location_id",
                "snapshot_date",
                "available_quantity",
                "reserved_quantity",
                "unit_cost"
            ] + LINEAGE
        ),

    "missing_inventory_snapshots":
        missing_inventory_snapshots.copy(),

    "campaign_spend":
        select_existing(
            campaign_spend,
            [
                "spend_date",
                "campaign_id",
                "channel",
                "spend_amount"
            ] + LINEAGE
        ),

    "payment_state":
        select_existing(
            payment_state,
            [
                "payment_id",
                "order_id",
                "amount",
                "current_status",
                "status_occurred_at_utc",
                "status_event_id"
            ] + LINEAGE
        ),

    "refund_state":
        select_existing(
            refund_state,
            [
                "refund_id",
                "order_id",
                "amount",
                "current_status",
                "status_occurred_at_utc",
                "status_event_id"
            ] + LINEAGE
        ),

    "return_state":
        select_existing(
            return_state,
            [
                "return_id",
                "order_id",
                "current_status",
                "status_occurred_at_utc",
                "status_event_id"
            ] + LINEAGE
        ),

    "support_state":
        select_existing(
            support_state,
            [
                "ticket_id",
                "customer_id",
                "order_id",
                "reason",
                "current_status",
                "status_occurred_at_utc",
                "status_event_id"
            ] + LINEAGE
        ),
}


for name, df in silver_tables.items():

    print(
        f"{name:24s} {len(df):>8,}"
    )


# ============================================================
# 15. TULIS KE SCHEMA SILVER
# ============================================================

with engine.begin() as connection:

    connection.execute(
        text(
            "CREATE SCHEMA IF NOT EXISTS silver"
        )
    )


# Replace hanya tabel turunan Silver,
# Bronze tidak disentuh.

for table_name, df in silver_tables.items():

    df.to_sql(
        table_name,
        con=engine,
        schema="silver",
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )

    print(
        f"[SILVER] {table_name}: "
        f"{len(df)} rows"
    )


rejected_records.to_sql(
    "rejected_records",
    con=engine,
    schema="silver",
    if_exists="replace",
    index=False,
    chunksize=5000,
    method="multi",
)


print(
    "\nSilver layer berhasil disimpan."
)


# ============================================================
# 16. INDEX DAN CONSTRAINT PENTING
# ============================================================

index_sql = [

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_customers
    ON silver.customers(customer_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_products
    ON silver.products(product_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_orders
    ON silver.orders(order_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_order_items
    ON silver.order_items(order_item_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_payment_events
    ON silver.payment_events(event_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_refund_events
    ON silver.refund_events(event_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_return_events
    ON silver.return_events(event_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_support_events
    ON silver.support_events(event_id)
    """,

    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    ux_silver_web_events
    ON silver.web_events(event_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS
    ix_silver_orders_customer
    ON silver.orders(customer_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS
    ix_silver_order_items_order
    ON silver.order_items(order_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS
    ix_silver_inventory_product_date
    ON silver.inventory_snapshots(
        product_id,
        snapshot_date
    )
    """,

    """
    CREATE INDEX IF NOT EXISTS
    ix_silver_rejected_reason
    ON silver.rejected_records(
        rejection_reason
    )
    """,
]


with engine.begin() as connection:

    for sql in index_sql:
        connection.execute(
            text(sql)
        )


print(
    "Index Silver berhasil dibuat."
)


# ============================================================
# 17. QUALITY CHECKS SILVER
# ============================================================

quality_checks = {}


# 1. Tidak boleh ada duplicate business key

quality_checks["duplicate_orders"] = int(
    silver_tables[
        "orders"
    ]["order_id"].duplicated().sum()
)


quality_checks["duplicate_order_items"] = int(
    silver_tables[
        "order_items"
    ]["order_item_id"].duplicated().sum()
)


quality_checks["duplicate_payment_events"] = int(
    silver_tables[
        "payment_events"
    ]["event_id"].duplicated().sum()
)


quality_checks["duplicate_refund_events"] = int(
    silver_tables[
        "refund_events"
    ]["event_id"].duplicated().sum()
)


quality_checks["duplicate_return_events"] = int(
    silver_tables[
        "return_events"
    ]["event_id"].duplicated().sum()
)


quality_checks["duplicate_support_events"] = int(
    silver_tables[
        "support_events"
    ]["event_id"].duplicated().sum()
)


quality_checks["duplicate_web_events"] = int(
    silver_tables[
        "web_events"
    ]["event_id"].duplicated().sum()
)


# 2. Quantity Silver harus positif.

quality_checks["non_positive_quantity"] = int(
    (
        silver_tables[
            "order_items"
        ]["quantity"] <= 0
    ).sum()
)


# 3. Foreign key utama harus valid.

quality_checks["orphan_order_customer"] = int(
    (
        ~silver_tables[
            "orders"
        ]["customer_id"].isin(
            silver_tables[
                "customers"
            ]["customer_id"]
        )
    ).sum()
)


quality_checks["orphan_item_order"] = int(
    (
        ~silver_tables[
            "order_items"
        ]["order_id"].isin(
            silver_tables[
                "orders"
            ]["order_id"]
        )
    ).sum()
)


quality_checks["orphan_item_product"] = int(
    (
        ~silver_tables[
            "order_items"
        ]["product_id"].isin(
            silver_tables[
                "products"
            ]["product_id"]
        )
    ).sum()
)


quality_df = pd.DataFrame(
    [
        {
            "check": k,
            "failed_rows": v
        }
        for k, v
        in quality_checks.items()
    ]
)


quality_df["passed"] = (
    quality_df[
        "failed_rows"
    ].eq(0)
)


print()
print("SILVER QUALITY CHECKS")
print(
    quality_df.to_string(
        index=False
    )
)


if not quality_df["passed"].all():

    failed = quality_df.loc[
        ~quality_df["passed"],
        "check"
    ].tolist()

    raise RuntimeError(
        f"Silver quality gate gagal: {failed}"
    )


print(
    "QUALITY GATE PASSED"
)


# ============================================================
# 18. RECONCILIATION BRONZE -> SILVER
# ============================================================

reconciliation = pd.DataFrame([
    {
        "metric":
            "bronze_total_rows",

        "value":
            len(bronze),
    },

    {
        "metric":
            "silver_accepted_rows",

        "value":
            sum(
                len(df)
                for name, df
                in silver_tables.items()
                if not name.endswith(
                    "_state"
                )
            ),
    },

    {
        "metric":
            "silver_rejected_rows",

        "value":
            len(rejected_records),
    },

    {
        "metric":
            "late_payment_events",

        "value":
            int(
                silver_tables[
                    "payment_events"
                ][
                    "is_late_arriving"
                ].sum()
            ),
    },

    {
        "metric":
            "late_refund_events",

        "value":
            int(
                silver_tables[
                    "refund_events"
                ][
                    "is_late_arriving"
                ].sum()
            ),
    },

    {
        "metric":
            "late_return_events",

        "value":
            int(
                silver_tables[
                    "return_events"
                ][
                    "is_late_arriving"
                ].sum()
            ),
    },

    {
        "metric":
            "late_support_events",

        "value":
            int(
                silver_tables[
                    "support_events"
                ][
                    "is_late_arriving"
                ].sum()
            ),
    },

    {
        "metric":
            "late_web_events",

        "value":
            int(
                silver_tables[
                    "web_events"
                ][
                    "is_late_arriving"
                ].sum()
            ),
    },
])


print()
print("RECONCILIATION")
print(
    reconciliation.to_string(
        index=False
    )
)


print(
    "\nRejected by reason:"
)


rejected_by_reason = (
    rejected_records
    .groupby(
        [
            "source_entity",
            "rejection_reason"
        ]
    )
    .size()
    .reset_index(
        name="row_count"
    )
    .sort_values(
        [
            "source_entity",
            "row_count"
        ],
        ascending=[
            True,
            False
        ]
    )
)


print(
    rejected_by_reason.to_string(
        index=False
    )
)


# ============================================================
# 19. VALIDASI HASIL LANGSUNG DARI POSTGRESQL
# ============================================================

table_counts = pd.read_sql(
    text("""
        SELECT
            table_name,
            (
                xpath(
                    '/row/c/text()',
                    query_to_xml(
                        format(
                            'SELECT count(*) AS c FROM silver.%I',
                            table_name
                        ),
                        false,
                        true,
                        ''
                    )
                )
            )[1]::text::bigint AS row_count
        FROM information_schema.tables
        WHERE table_schema = 'silver'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """),
    engine,
)


print()
print("SILVER TABLE COUNTS")
print(
    table_counts.to_string(
        index=False
    )
)


sample_rejected = pd.read_sql(
    text("""
        SELECT *
        FROM silver.rejected_records
        ORDER BY
            rejected_at_utc DESC,
            source_file,
            source_row_number
        LIMIT 20
    """),
    engine,
)


print()
print("SAMPLE REJECTED RECORDS")
print(
    sample_rejected.to_string(
        index=False
    )
)


# ============================================================
# 20. FINAL QUALITY CHECK - SILVER LAYER
# ============================================================

quality_results = []


def add_check(
    check_name,
    actual,
    expected,
    passed
):

    quality_results.append({
        "check_name":
            check_name,

        "actual":
            actual,

        "expected":
            expected,

        "status":
            "PASS"
            if passed
            else "FAIL"
    })


# ------------------------------------------------------------
# 1. DUPLICATE ORDERS
# ------------------------------------------------------------

duplicate_orders = (
    orders[
        "order_id"
    ].duplicated().sum()
)


add_check(
    "Duplicate order_id",
    duplicate_orders,
    0,
    duplicate_orders == 0
)


# ------------------------------------------------------------
# 2. DUPLICATE EVENT ID
# ------------------------------------------------------------

event_tables = {
    "payment_events":
        payment_events,

    "refund_events":
        refund_events,

    "return_events":
        return_events,

    "support_events":
        support_events,

    "web_events":
        web_events
}


for table_name, df in event_tables.items():

    duplicate_events = (
        df[
            "event_id"
        ].duplicated().sum()
    )

    add_check(
        f"Duplicate event_id - {table_name}",
        duplicate_events,
        0,
        duplicate_events == 0
    )


# ------------------------------------------------------------
# 3. NEGATIVE / ZERO QUANTITY
# ------------------------------------------------------------

invalid_quantity = (
    pd.to_numeric(
        order_items[
            "quantity"
        ],
        errors="coerce"
    ) <= 0
).sum()


add_check(
    "Invalid order item quantity",
    invalid_quantity,
    0,
    invalid_quantity == 0
)


# ------------------------------------------------------------
# 4. INVALID PRODUCT FK
# ------------------------------------------------------------

invalid_product_fk = (
    ~order_items[
        "product_id"
    ].isin(
        products[
            "product_id"
        ]
    )
).sum()


add_check(
    "Invalid product FK",
    invalid_product_fk,
    0,
    invalid_product_fk == 0
)


# ------------------------------------------------------------
# 5. INVALID CUSTOMER FK
# ------------------------------------------------------------

invalid_customer_fk = (
    ~orders[
        "customer_id"
    ].isin(
        customers[
            "customer_id"
        ]
    )
).sum()


add_check(
    "Invalid customer FK",
    invalid_customer_fk,
    0,
    invalid_customer_fk == 0
)


# ------------------------------------------------------------
# 6. INVALID ORDER FK PADA ORDER ITEMS
# ------------------------------------------------------------

invalid_order_fk = (
    ~order_items[
        "order_id"
    ].isin(
        orders[
            "order_id"
        ]
    )
).sum()


add_check(
    "Invalid order FK - order_items",
    invalid_order_fk,
    0,
    invalid_order_fk == 0
)


# ------------------------------------------------------------
# 7. LINEAGE NULL
# ------------------------------------------------------------

base_tables_for_lineage = {
    name: df

    for name, df
    in silver_tables.items()

    if name not in {
        "payment_state",
        "refund_state",
        "return_state",
        "support_state",
        "missing_inventory_snapshots",
        "rejected_records"
    }
}


lineage_null = 0


for table_name, df in (
    base_tables_for_lineage.items()
):

    if "raw_record_id" in df.columns:

        lineage_null += (
            df[
                "raw_record_id"
            ].isna().sum()
        )


add_check(
    "Null Bronze lineage",
    lineage_null,
    0,
    lineage_null == 0
)


# ------------------------------------------------------------
# 8. BRONZE -> SILVER RECONCILIATION
# ------------------------------------------------------------

bronze_count = len(bronze)


accepted_silver_count = sum(
    len(df)

    for name, df
    in silver_tables.items()

    if name not in {
        "payment_state",
        "refund_state",
        "return_state",
        "support_state",
        "missing_inventory_snapshots",
        "rejected_records"
    }
)


rejected_count = len(
    rejected_records
)


reconciled_count = (
    accepted_silver_count
    + rejected_count
)


add_check(
    "Bronze to Silver reconciliation",
    reconciled_count,
    bronze_count,
    reconciled_count == bronze_count
)


# ------------------------------------------------------------
# 9. INVENTORY SNAPSHOT COMPLETENESS
# ------------------------------------------------------------

expected_inventory_count = len(
    expected_inventory
)

actual_inventory_count = len(
    actual_inventory
)

missing_inventory_count = len(
    missing_inventory_snapshots
)


inventory_completeness = round(
    (
        actual_inventory_count
        / expected_inventory_count
    ) * 100,
    2
)


# Missing inventory adalah anomaly yang memang
# ingin dideteksi, jadi tidak dijadikan hard FAIL.

quality_results.append({
    "check_name":
        "Inventory snapshot completeness",

    "actual":
        (
            f"{inventory_completeness}% "
            f"({missing_inventory_count} missing)"
        ),

    "expected":
        "Observed / reported",

    "status":
        "INFO"
})


# ------------------------------------------------------------
# 10. CANCELLED + CAPTURED PAYMENT
# ------------------------------------------------------------

cancelled_captured_count = len(
    cancelled_captured_orders
)


quality_results.append({
    "check_name":
        "Cancelled order with captured payment",

    "actual":
        cancelled_captured_count,

    "expected":
        "Observed / reported",

    "status":
        "INFO"
})


# ============================================================
# TAMPILKAN HASIL
# ============================================================

quality_report = pd.DataFrame(
    quality_results
)


print()
print("FINAL SILVER QUALITY REPORT")
print(
    quality_report.to_string(
        index=False
    )
)


# ============================================================
# FINAL QUALITY GATE
# ============================================================

failed_checks = quality_report[
    quality_report[
        "status"
    ] == "FAIL"
]


print()
print("=" * 60)
print("FINAL SILVER QUALITY GATE")
print("=" * 60)


print(
    "Bronze rows           :",
    bronze_count
)

print(
    "Accepted Silver rows  :",
    accepted_silver_count
)

print(
    "Rejected rows         :",
    rejected_count
)

print(
    "Reconciled rows       :",
    reconciled_count
)


print()


print(
    "Expected inventory    :",
    expected_inventory_count
)

print(
    "Actual inventory      :",
    actual_inventory_count
)

print(
    "Missing inventory     :",
    missing_inventory_count
)

print(
    "Inventory completeness:",
    f"{inventory_completeness}%"
)


print()


print(
    "Cancelled + captured :",
    cancelled_captured_count
)


print("=" * 60)


if len(failed_checks) == 0:

    print(
        "SILVER QUALITY GATE: PASS"
    )

    print(
        "Silver layer siap."
    )

else:

    print(
        "SILVER QUALITY GATE: FAIL"
    )

    print(
        "Jumlah check gagal:",
        len(failed_checks)
    )

    print(
        failed_checks.to_string(
            index=False
        )
    )

    raise RuntimeError(
        "Final Silver Quality Gate gagal: "
        + str(
            failed_checks[
                "check_name"
            ].tolist()
        )
    )


# ============================================================
# SELESAI
# ============================================================

engine.dispose()