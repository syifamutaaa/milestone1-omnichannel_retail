# ============================================================
# GOLD LAYER — OMNICHANNEL RETAIL
# ============================================================
# Pola:
# connect -> load Silver -> aggregate -> build Gold
# -> quality check -> write -> validate -> analytics -> engine
#
# Gold hanya membaca silver.*
# ============================================================


# ============================================================
# 1. IMPORTS DAN DATABASE CONNECTION
# ============================================================

import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


repo_root = Path.cwd()

if (
    not (repo_root / ".env").exists()
    and (repo_root.parent / ".env").exists()
):
    repo_root = repo_root.parent


load_dotenv(repo_root / ".env")


if all(
    os.getenv(k)
    for k in [
        "DB_USER",
        "DB_PASSWORD",
        "DB_HOST",
        "DB_NAME"
    ]
):

    engine = create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(
                os.getenv(
                    "DB_PORT",
                    "5432"
                )
            ),
            database=os.getenv("DB_NAME"),
        ),
        connect_args=(
            {
                "sslmode": "require"
            }
            if os.getenv(
                "DB_SSLMODE",
                ""
            ).lower() == "require"
            else {}
        ),
    )

else:

    engine = create_engine(
        "postgresql+psycopg2://"
        "postgres:postgres@localhost:5432/"
        "milestone1_omnichannel"
    )


with engine.connect() as connection:

    print(
        "Connected to:",
        connection.execute(
            text(
                "SELECT current_database()"
            )
        ).scalar()
    )


# ============================================================
# 2. LOAD SILVER TABLES
# ============================================================
#
# Tidak ada raw JSON / CSV yang dibaca pada Gold.
# Semua input berasal dari schema silver.
# ============================================================


def load_silver(table_name):

    return pd.read_sql(
        text(
            f"SELECT * FROM silver.{table_name}"
        ),
        engine
    )


silver_names = [
    "orders",
    "order_items",
    "order_promotions",
    "payment_state",
    "refund_state",
    "return_state",
    "payment_events",
    "refund_events",
    "return_events",
    "support_events",
    "web_events",
    "inventory_snapshots",
    "campaign_spend",
    "product_categories",
]


S = {
    name: load_silver(name)
    for name in silver_names
}


for name, df in S.items():

    print(
        f"{name:24s} {len(df):>8,}"
    )


if S["orders"].empty:

    raise RuntimeError(
        "No Silver orders found. "
        "Run Bronze and Silver first."
    )


# ============================================================
# 3. GOLD CONTRACT DAN METRIC DEFINITIONS
# ============================================================
#
# Grain:
#
# gold.order_360
#   satu baris per order
#
# gold.customer_daily
#   satu customer per business date
#
# gold.product_daily
#   satu product per business date
#
# gold.channel_campaign_daily
#   satu date + channel + campaign
#
# gold.executive_kpis_daily
#   satu business date
#
#
# Metric:
#
# net_revenue =
#   gross_merchandise_value
#   - discount_amount
#   + shipping_revenue
#   - refunded_amount
#
# refund_rate =
#   refunded orders /
#   orders with captured payment
#
# return_rate =
#   returned orders /
#   resolved orders
#
# roas =
#   attributed net revenue /
#   campaign spend
#
#
# Anti fact multiplication:
# Semua fact/event diagregasi terlebih dahulu
# sebelum join.
# ============================================================


# ============================================================
# 4. NORMALISASI TIPE DAN PILIH PIPELINE_RUN_ID
# ============================================================

orders = S["orders"].copy()
order_items = S["order_items"].copy()
order_promotions = S["order_promotions"].copy()

payment_state = S["payment_state"].copy()
refund_state = S["refund_state"].copy()
return_state = S["return_state"].copy()

payment_events = S["payment_events"].copy()
refund_events = S["refund_events"].copy()
return_events = S["return_events"].copy()
support_events = S["support_events"].copy()
web_events = S["web_events"].copy()

inventory = S["inventory_snapshots"].copy()
campaign_spend = S["campaign_spend"].copy()
product_categories = S["product_categories"].copy()


orders["ordered_at_utc"] = pd.to_datetime(
    orders["ordered_at_utc"],
    utc=True
)

orders["updated_at_utc"] = pd.to_datetime(
    orders["updated_at_utc"],
    utc=True
)

orders["order_date"] = (
    orders["ordered_at_utc"].dt.date
)


for df in [
    payment_events,
    refund_events,
    return_events,
    support_events,
    web_events
]:

    df["occurred_at_utc"] = pd.to_datetime(
        df["occurred_at_utc"],
        utc=True
    )


for df in [
    payment_state,
    refund_state,
    return_state
]:

    df["status_occurred_at_utc"] = (
        pd.to_datetime(
            df["status_occurred_at_utc"],
            utc=True
        )
    )


inventory["snapshot_date"] = (
    pd.to_datetime(
        inventory["snapshot_date"]
    ).dt.date
)


campaign_spend["spend_date"] = (
    pd.to_datetime(
        campaign_spend["spend_date"]
    ).dt.date
)


product_categories["valid_from_utc"] = (
    pd.to_datetime(
        product_categories[
            "valid_from_utc"
        ],
        utc=True
    )
)


product_categories["valid_to_utc"] = (
    pd.to_datetime(
        product_categories[
            "valid_to_utc"
        ],
        utc=True
    )
)


pipeline_run_id = os.getenv(
    "PIPELINE_RUN_ID"
)

if not pipeline_run_id:
    pipeline_run_id = str(
        orders[
            "pipeline_run_id"
        ].mode().iloc[0]
    )


print(
    "Pipeline run ID:",
    pipeline_run_id
)


# ============================================================
# 5. BUILD GOLD.ORDER_360
# ============================================================


# ------------------------------------------------------------
# ITEM AGGREGATE — ONE ROW PER ORDER
# ------------------------------------------------------------

item_work = order_items.copy()


item_work["line_gmv"] = (
    item_work["quantity"]
    * item_work["unit_price"]
)


item_agg = (
    item_work
    .groupby(
        "order_id",
        as_index=False
    )
    .agg(
        gross_merchandise_value=(
            "line_gmv",
            "sum"
        ),
        item_discount_amount=(
            "item_discount_amount",
            "sum"
        ),
        item_count=(
            "order_item_id",
            "nunique"
        ),
        unit_quantity=(
            "quantity",
            "sum"
        ),
    )
)


# ------------------------------------------------------------
# PROMOTION AGGREGATE — ONE ROW PER ORDER
# ------------------------------------------------------------

promo_agg = (
    order_promotions
    .groupby(
        "order_id",
        as_index=False
    )
    .agg(
        promotion_discount_amount=(
            "discount_amount",
            "sum"
        ),
        promotion_count=(
            "promotion_id",
            "nunique"
        ),
    )
)


# ------------------------------------------------------------
# CAPTURED PAYMENT — ONE ROW PER ORDER
# ------------------------------------------------------------

captured = payment_state[
    payment_state[
        "current_status"
    ].eq(
        "PAYMENT_CAPTURED"
    )
].copy()


payment_agg = (
    captured
    .groupby(
        "order_id",
        as_index=False
    )
    .agg(
        captured_payment_amount=(
            "amount",
            "sum"
        )
    )
)


# ------------------------------------------------------------
# PAYMENT STATUS PER ORDER
# ------------------------------------------------------------

payment_priority = {
    "PAYMENT_FAILED": 1,
    "PAYMENT_AUTHORIZED": 2,
    "PAYMENT_CAPTURED": 3,
}


payment_status = payment_state[
    [
        "order_id",
        "current_status"
    ]
].copy()


payment_status["priority"] = (
    payment_status[
        "current_status"
    ]
    .map(payment_priority)
    .fillna(0)
)


payment_status = (
    payment_status
    .sort_values(
        [
            "order_id",
            "priority"
        ]
    )
    .groupby(
        "order_id",
        as_index=False
    )
    .tail(1)
    [
        [
            "order_id",
            "current_status"
        ]
    ]
    .rename(
        columns={
            "current_status":
                "payment_status"
        }
    )
)


# ------------------------------------------------------------
# COMPLETED REFUND — ONE ROW PER ORDER
# ------------------------------------------------------------

completed_refund = refund_state[
    refund_state[
        "current_status"
    ].eq(
        "REFUND_COMPLETED"
    )
].copy()


refund_agg = (
    completed_refund
    .groupby(
        "order_id",
        as_index=False
    )
    .agg(
        refunded_amount=(
            "amount",
            "sum"
        )
    )
)


# ------------------------------------------------------------
# RETURN STATE — ONE ROW PER ORDER
# ------------------------------------------------------------

return_status = (
    return_state
    .sort_values(
        [
            "order_id",
            "status_occurred_at_utc",
            "status_event_id"
        ]
    )
    .groupby(
        "order_id",
        as_index=False
    )
    .tail(1)
    [
        [
            "order_id",
            "current_status"
        ]
    ]
    .rename(
        columns={
            "current_status":
                "return_status"
        }
    )
)


# ------------------------------------------------------------
# FIRST ORDER FLAG
# ------------------------------------------------------------

first_order_date = (
    orders
    .groupby(
        "customer_id"
    )[
        "ordered_at_utc"
    ]
    .transform("min")
)


order_base = orders.copy()


order_base["first_order_flag"] = (
    order_base[
        "ordered_at_utc"
    ].eq(first_order_date)
)


# ------------------------------------------------------------
# BUILD ORDER 360
# ------------------------------------------------------------

order_360 = (
    order_base[
        [
            "order_id",
            "customer_id",
            "order_date",
            "sales_channel",
            "store_id",
            "shipping_revenue",
            "status",
            "first_order_flag"
        ]
    ]
    .merge(
        item_agg,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
    .merge(
        promo_agg,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
    .merge(
        payment_agg,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
    .merge(
        refund_agg,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
    .merge(
        payment_status,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
    .merge(
        return_status,
        on="order_id",
        how="left",
        validate="one_to_one"
    )
)


for col in [
    "gross_merchandise_value",
    "item_discount_amount",
    "promotion_discount_amount",
    "captured_payment_amount",
    "refunded_amount"
]:

    order_360[col] = (
        pd.to_numeric(
            order_360[col],
            errors="coerce"
        )
        .fillna(0.0)
    )


order_360["item_count"] = (
    order_360[
        "item_count"
    ]
    .fillna(0)
    .astype("int64")
)


order_360["unit_quantity"] = (
    order_360[
        "unit_quantity"
    ]
    .fillna(0)
    .astype("int64")
)


order_360["promotion_count"] = (
    order_360[
        "promotion_count"
    ]
    .fillna(0)
    .astype("int64")
)


order_360["discount_amount"] = (
    order_360[
        "item_discount_amount"
    ]
    + order_360[
        "promotion_discount_amount"
    ]
)


order_360["net_revenue"] = (
    order_360[
        "gross_merchandise_value"
    ]
    - order_360[
        "discount_amount"
    ]
    + order_360[
        "shipping_revenue"
    ]
    - order_360[
        "refunded_amount"
    ]
)


order_360["order_status"] = (
    order_360["status"]
)


order_360["payment_status"] = (
    order_360[
        "payment_status"
    ].fillna(
        "NO_PAYMENT"
    )
)


order_360["return_status"] = (
    order_360[
        "return_status"
    ].fillna(
        "NO_RETURN"
    )
)


order_360["pipeline_run_id"] = (
    pipeline_run_id
)


ORDER_360_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "sales_channel",
    "store_id",
    "gross_merchandise_value",
    "discount_amount",
    "shipping_revenue",
    "captured_payment_amount",
    "refunded_amount",
    "net_revenue",
    "item_count",
    "unit_quantity",
    "order_status",
    "payment_status",
    "return_status",
    "promotion_count",
    "first_order_flag",
    "pipeline_run_id"
]


order_360 = order_360[
    ORDER_360_COLUMNS
].copy()


print(
    "order_360 rows:",
    len(order_360)
)

print(
    order_360.head().to_string(
        index=False
    )
)


# ============================================================
# 6. BUILD GOLD.CUSTOMER_DAILY
# ============================================================


customer_orders = (
    order_360
    .groupby(
        [
            "customer_id",
            "order_date"
        ],
        as_index=False
    )
    .agg(
        order_count=(
            "order_id",
            "nunique"
        ),
        unit_quantity=(
            "unit_quantity",
            "sum"
        ),
        gross_revenue=(
            "gross_merchandise_value",
            "sum"
        ),
        net_revenue=(
            "net_revenue",
            "sum"
        ),
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


# ------------------------------------------------------------
# ACTIVE CHANNEL
# ------------------------------------------------------------

channel_counts = (
    order_360
    .groupby(
        [
            "customer_id",
            "order_date",
            "sales_channel"
        ],
        as_index=False
    )
    .agg(
        channel_orders=(
            "order_id",
            "nunique"
        )
    )
    .sort_values(
        [
            "customer_id",
            "order_date",
            "channel_orders",
            "sales_channel"
        ],
        ascending=[
            True,
            True,
            False,
            True
        ]
    )
)


active_channel = (
    channel_counts
    .groupby(
        [
            "customer_id",
            "order_date"
        ],
        as_index=False
    )
    .head(1)
    [
        [
            "customer_id",
            "order_date",
            "sales_channel"
        ]
    ]
    .rename(
        columns={
            "order_date":
                "metric_date",

            "sales_channel":
                "active_channel"
        }
    )
)


# ------------------------------------------------------------
# CUSTOMER REFUND
# ------------------------------------------------------------

customer_refund = (
    completed_refund
    .merge(
        order_360[
            [
                "order_id",
                "customer_id"
            ]
        ],
        on="order_id",
        how="left",
        validate="many_to_one"
    )
)


customer_refund["metric_date"] = (
    customer_refund[
        "status_occurred_at_utc"
    ].dt.date
)


customer_refund = (
    customer_refund
    .groupby(
        [
            "customer_id",
            "metric_date"
        ],
        as_index=False
    )
    .agg(
        refund_amount=(
            "amount",
            "sum"
        )
    )
)


# ------------------------------------------------------------
# CUSTOMER RETURN
# ------------------------------------------------------------

customer_return = (
    return_state
    .merge(
        order_360[
            [
                "order_id",
                "customer_id"
            ]
        ],
        on="order_id",
        how="left",
        validate="many_to_one"
    )
)


customer_return["metric_date"] = (
    customer_return[
        "status_occurred_at_utc"
    ].dt.date
)


customer_return = (
    customer_return
    .groupby(
        [
            "customer_id",
            "metric_date"
        ],
        as_index=False
    )
    .agg(
        return_count=(
            "return_id",
            "nunique"
        )
    )
)


# ------------------------------------------------------------
# CUSTOMER SUPPORT
# ------------------------------------------------------------

support_created = support_events[
    support_events[
        "event_type"
    ].eq(
        "SUPPORT_TICKET_CREATED"
    )
].copy()


support_created["metric_date"] = (
    support_created[
        "occurred_at_utc"
    ].dt.date
)


customer_support = (
    support_created
    .groupby(
        [
            "customer_id",
            "metric_date"
        ],
        as_index=False
    )
    .agg(
        support_contacts=(
            "ticket_id",
            "nunique"
        )
    )
)


# ------------------------------------------------------------
# UNION CUSTOMER-DATE KEYS
# ------------------------------------------------------------

keys = (
    pd.concat(
        [
            customer_orders[
                [
                    "customer_id",
                    "metric_date"
                ]
            ],

            customer_refund[
                [
                    "customer_id",
                    "metric_date"
                ]
            ],

            customer_return[
                [
                    "customer_id",
                    "metric_date"
                ]
            ],

            customer_support[
                [
                    "customer_id",
                    "metric_date"
                ]
            ],
        ],
        ignore_index=True
    )
    .dropna()
    .drop_duplicates()
)


customer_daily = (
    keys
    .merge(
        customer_orders,
        on=[
            "customer_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        customer_refund,
        on=[
            "customer_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        customer_return,
        on=[
            "customer_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        customer_support,
        on=[
            "customer_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        active_channel,
        on=[
            "customer_id",
            "metric_date"
        ],
        how="left"
    )
)


for col in [
    "order_count",
    "unit_quantity",
    "return_count",
    "support_contacts"
]:

    customer_daily[col] = (
        customer_daily[col]
        .fillna(0)
        .astype("int64")
    )


for col in [
    "gross_revenue",
    "net_revenue",
    "refund_amount"
]:

    customer_daily[col] = (
        customer_daily[col]
        .fillna(0.0)
    )


first_customer_date = (
    order_360
    .groupby(
        "customer_id"
    )[
        "order_date"
    ]
    .min()
    .to_dict()
)


customer_daily[
    "new_customer_flag"
] = customer_daily.apply(
    lambda r:
        first_customer_date.get(
            r["customer_id"]
        )
        == r["metric_date"],
    axis=1
)


order_dates_by_customer = (
    order_360[
        [
            "customer_id",
            "order_date"
        ]
    ]
    .drop_duplicates()
    .sort_values(
        [
            "customer_id",
            "order_date"
        ]
    )
)


customer_daily[
    "repeat_customer_flag"
] = customer_daily.apply(
    lambda r: (
        (
            order_dates_by_customer[
                "customer_id"
            ].eq(
                r["customer_id"]
            )
        )
        & (
            order_dates_by_customer[
                "order_date"
            ]
            < r["metric_date"]
        )
    ).any(),
    axis=1
)


customer_daily[
    "pipeline_run_id"
] = pipeline_run_id


CUSTOMER_DAILY_COLUMNS = [
    "customer_id",
    "metric_date",
    "order_count",
    "unit_quantity",
    "gross_revenue",
    "net_revenue",
    "refund_amount",
    "return_count",
    "support_contacts",
    "active_channel",
    "new_customer_flag",
    "repeat_customer_flag",
    "pipeline_run_id"
]


customer_daily = customer_daily[
    CUSTOMER_DAILY_COLUMNS
].copy()


print(
    "customer_daily rows:",
    len(customer_daily)
)

print(
    customer_daily.head().to_string(
        index=False
    )
)


# ============================================================
# 7. BUILD GOLD.PRODUCT_DAILY
# ============================================================


product_lines = (
    item_work
    .merge(
        order_360[
            [
                "order_id",
                "order_date",
                "gross_merchandise_value",
                "discount_amount",
                "shipping_revenue",
                "refunded_amount"
            ]
        ],
        on="order_id",
        how="inner",
        validate="many_to_one",
        suffixes=(
            "_line",
            "_order"
        )
    )
)


# ------------------------------------------------------------
# ALOKASI ORDER VALUE KE PRODUCT
# ------------------------------------------------------------

order_total_quantity = (
    product_lines
    .groupby(
        "order_id"
    )[
        "quantity"
    ]
    .transform("sum")
)


line_count = (
    product_lines
    .groupby(
        "order_id"
    )[
        "order_item_id"
    ]
    .transform("count")
)


# ------------------------------------------------------------
# DEBUG ORD-000011
# ------------------------------------------------------------

order_id_debug = "ORD-000011"


print(
    "\n=== ORDER 360 ==="
)

print(
    order_360[
        order_360[
            "order_id"
        ].eq(order_id_debug)
    ].to_string(
        index=False
    )
)


print(
    "\n=== SILVER ORDER ITEMS ==="
)

print(
    order_items[
        order_items[
            "order_id"
        ].eq(order_id_debug)
    ].to_string(
        index=False
    )
)


print(
    "\n=== PRODUCT LINES ==="
)

print(
    product_lines[
        product_lines[
            "order_id"
        ].eq(order_id_debug)
    ].to_string(
        index=False
    )
)


print(
    "\nJumlah order_items:",
    len(
        order_items[
            order_items[
                "order_id"
            ].eq(order_id_debug)
        ]
    )
)


print(
    "Jumlah product_lines:",
    len(
        product_lines[
            product_lines[
                "order_id"
            ].eq(order_id_debug)
        ]
    )
)


# ------------------------------------------------------------
# GMV SHARE
# ------------------------------------------------------------
#
# GMV > 0:
# alokasi berdasarkan kontribusi GMV.
#
# GMV = 0:
# alokasi rata ke setiap product line.
# ------------------------------------------------------------

product_lines[
    "gmv_share"
] = np.where(
    product_lines[
        "gross_merchandise_value"
    ].gt(0),

    product_lines[
        "line_gmv"
    ]
    / product_lines[
        "gross_merchandise_value"
    ],

    1.0 / line_count
)


product_lines[
    "allocated_discount"
] = (
    product_lines[
        "discount_amount"
    ]
    * product_lines[
        "gmv_share"
    ]
)


product_lines[
    "allocated_shipping"
] = (
    product_lines[
        "shipping_revenue"
    ]
    * product_lines[
        "gmv_share"
    ]
)


product_lines[
    "allocated_refund"
] = (
    product_lines[
        "refunded_amount"
    ]
    * product_lines[
        "gmv_share"
    ]
)


product_lines[
    "line_net_revenue"
] = (
    product_lines[
        "line_gmv"
    ]
    - product_lines[
        "allocated_discount"
    ]
    + product_lines[
        "allocated_shipping"
    ]
    - product_lines[
        "allocated_refund"
    ]
)


# ------------------------------------------------------------
# ESTIMATED REFUNDED UNITS
# ------------------------------------------------------------

pre_refund_value = (
    product_lines[
        "gross_merchandise_value"
    ]
    - product_lines[
        "discount_amount"
    ]
    + product_lines[
        "shipping_revenue"
    ]
)


product_lines[
    "refund_ratio"
] = np.where(
    pre_refund_value.gt(0),

    (
        product_lines[
            "refunded_amount"
        ]
        / pre_refund_value
    ).clip(
        0,
        1
    ),

    0.0
)


product_lines[
    "estimated_refunded_units"
] = (
    product_lines[
        "quantity"
    ]
    * product_lines[
        "refund_ratio"
    ]
)


product_sales = (
    product_lines
    .groupby(
        [
            "product_id",
            "order_date"
        ],
        as_index=False
    )
    .agg(
        units_sold=(
            "quantity",
            "sum"
        ),
        order_count=(
            "order_id",
            "nunique"
        ),
        gross_revenue=(
            "line_gmv",
            "sum"
        ),
        net_revenue=(
            "line_net_revenue",
            "sum"
        ),
        refunded_units_float=(
            "estimated_refunded_units",
            "sum"
        ),
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


product_sales[
    "refunded_units"
] = (
    product_sales[
        "refunded_units_float"
    ]
    .round()
    .astype("int64")
)


product_sales = (
    product_sales.drop(
        columns=
            "refunded_units_float"
    )
)


# ------------------------------------------------------------
# PRODUCT PROMOTION COUNT
# ------------------------------------------------------------

order_product = (
    order_items[
        [
            "order_id",
            "product_id"
        ]
    ]
    .drop_duplicates()
)


product_promo = (
    order_product
    .merge(
        order_promotions[
            [
                "order_id",
                "promotion_id"
            ]
        ].drop_duplicates(),
        on="order_id",
        how="inner",
        validate="many_to_many"
    )
    .merge(
        order_360[
            [
                "order_id",
                "order_date"
            ]
        ],
        on="order_id",
        how="left",
        validate="many_to_one"
    )
    .groupby(
        [
            "product_id",
            "order_date"
        ],
        as_index=False
    )
    .agg(
        promotion_count=(
            "promotion_id",
            "nunique"
        )
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


# ------------------------------------------------------------
# INVENTORY DAILY
# ------------------------------------------------------------

inventory_daily = (
    inventory
    .groupby(
        [
            "product_id",
            "snapshot_date"
        ],
        as_index=False
    )
    .agg(
        available_inventory=(
            "available_quantity",
            "sum"
        ),
        reserved_inventory=(
            "reserved_quantity",
            "sum"
        ),
    )
    .rename(
        columns={
            "snapshot_date":
                "metric_date"
        }
    )
)


inventory_daily[
    "stockout_flag"
] = inventory_daily[
    "available_inventory"
].eq(0)


# ------------------------------------------------------------
# TEMPORAL CATEGORY
# ------------------------------------------------------------

def category_for_product_date(
    product_id,
    metric_date
):

    d = pd.Timestamp(
        metric_date,
        tz="UTC"
    )

    match = product_categories[
        product_categories[
            "product_id"
        ].eq(product_id)
        & product_categories[
            "valid_from_utc"
        ].le(d)
        & product_categories[
            "valid_to_utc"
        ].ge(d)
    ]

    if match.empty:
        return None

    return (
        match
        .sort_values(
            "valid_from_utc"
        )
        .iloc[-1][
            "category_name"
        ]
    )


# ------------------------------------------------------------
# UNION PRODUCT KEYS
# ------------------------------------------------------------

product_keys = (
    pd.concat(
        [
            product_sales[
                [
                    "product_id",
                    "metric_date"
                ]
            ],

            inventory_daily[
                [
                    "product_id",
                    "metric_date"
                ]
            ],
        ],
        ignore_index=True
    )
    .drop_duplicates()
)


product_daily = (
    product_keys
    .merge(
        product_sales,
        on=[
            "product_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        inventory_daily,
        on=[
            "product_id",
            "metric_date"
        ],
        how="left"
    )
    .merge(
        product_promo,
        on=[
            "product_id",
            "metric_date"
        ],
        how="left"
    )
)


for col in [
    "units_sold",
    "order_count",
    "refunded_units",
    "promotion_count"
]:

    product_daily[col] = (
        product_daily[col]
        .fillna(0)
        .astype("int64")
    )


for col in [
    "gross_revenue",
    "net_revenue"
]:

    product_daily[col] = (
        product_daily[col]
        .fillna(0.0)
    )


product_daily[
    "active_category"
] = product_daily.apply(
    lambda r:
        category_for_product_date(
            r["product_id"],
            r["metric_date"]
        ),
    axis=1
)


product_daily[
    "stockout_flag"
] = product_daily[
    "stockout_flag"
].astype(
    "boolean"
)


product_daily[
    "pipeline_run_id"
] = pipeline_run_id


PRODUCT_DAILY_COLUMNS = [
    "product_id",
    "metric_date",
    "active_category",
    "units_sold",
    "order_count",
    "gross_revenue",
    "net_revenue",
    "refunded_units",
    "available_inventory",
    "reserved_inventory",
    "stockout_flag",
    "promotion_count",
    "pipeline_run_id"
]


product_daily = product_daily[
    PRODUCT_DAILY_COLUMNS
].copy()


print(
    "product_daily rows:",
    len(product_daily)
)

print(
    product_daily.head().to_string(
        index=False
    )
)


# ============================================================
# 8. BUILD GOLD.CHANNEL_CAMPAIGN_DAILY
# ============================================================


web_campaign = web_events[
    web_events[
        "campaign_id"
    ].notna()
].copy()


web_campaign[
    "metric_date"
] = web_campaign[
    "occurred_at_utc"
].dt.date


web_campaign[
    "sales_channel"
] = web_campaign[
    "channel"
]


sessions = (
    web_campaign
    .groupby(
        [
            "metric_date",
            "sales_channel",
            "campaign_id"
        ],
        as_index=False
    )
    .agg(
        total_sessions=(
            "session_id",
            "nunique"
        )
    )
)


# ------------------------------------------------------------
# PURCHASE ATTRIBUTION
# ------------------------------------------------------------

purchase_attr = web_campaign[
    web_campaign[
        "event_type"
    ].eq(
        "PURCHASE"
    )
    & web_campaign[
        "order_id"
    ].notna()
].copy()


purchase_attr = (
    purchase_attr
    .drop_duplicates(
        [
            "metric_date",
            "sales_channel",
            "campaign_id",
            "order_id"
        ]
    )
)


# Ambil hanya key atribusi.
# customer_id diambil dari order_360.

purchase_attr = purchase_attr[
    [
        "metric_date",
        "sales_channel",
        "campaign_id",
        "order_id"
    ]
].copy()


attr_orders = (
    purchase_attr
    .merge(
        order_360[
            [
                "order_id",
                "customer_id",
                "gross_merchandise_value",
                "net_revenue",
                "refunded_amount"
            ]
        ],
        on="order_id",
        how="inner",
        validate="many_to_one"
    )
)


attribution = (
    attr_orders
    .groupby(
        [
            "metric_date",
            "sales_channel",
            "campaign_id"
        ],
        as_index=False
    )
    .agg(
        attributed_orders=(
            "order_id",
            "nunique"
        ),
        attributed_customers=(
            "customer_id",
            "nunique"
        ),
        gross_revenue=(
            "gross_merchandise_value",
            "sum"
        ),
        net_revenue=(
            "net_revenue",
            "sum"
        ),
        refunds=(
            "refunded_amount",
            "sum"
        ),
    )
)


spend = (
    campaign_spend
    .groupby(
        [
            "spend_date",
            "channel",
            "campaign_id"
        ],
        as_index=False
    )
    .agg(
        campaign_spend=(
            "spend_amount",
            "sum"
        )
    )
    .rename(
        columns={
            "spend_date":
                "metric_date",

            "channel":
                "sales_channel"
        }
    )
)


campaign_keys = (
    pd.concat(
        [
            spend[
                [
                    "metric_date",
                    "sales_channel",
                    "campaign_id"
                ]
            ],

            sessions[
                [
                    "metric_date",
                    "sales_channel",
                    "campaign_id"
                ]
            ],

            attribution[
                [
                    "metric_date",
                    "sales_channel",
                    "campaign_id"
                ]
            ],
        ],
        ignore_index=True
    )
    .drop_duplicates()
)


channel_campaign_daily = (
    campaign_keys
    .merge(
        spend,
        on=[
            "metric_date",
            "sales_channel",
            "campaign_id"
        ],
        how="left"
    )
    .merge(
        sessions,
        on=[
            "metric_date",
            "sales_channel",
            "campaign_id"
        ],
        how="left"
    )
    .merge(
        attribution,
        on=[
            "metric_date",
            "sales_channel",
            "campaign_id"
        ],
        how="left"
    )
)


for col in [
    "campaign_spend",
    "gross_revenue",
    "net_revenue",
    "refunds"
]:

    channel_campaign_daily[col] = (
        channel_campaign_daily[col]
        .fillna(0.0)
    )


for col in [
    "attributed_orders",
    "attributed_customers",
    "total_sessions"
]:

    channel_campaign_daily[col] = (
        channel_campaign_daily[col]
        .fillna(0)
        .astype("int64")
    )


channel_campaign_daily[
    "roas"
] = np.where(
    channel_campaign_daily[
        "campaign_spend"
    ].gt(0),

    channel_campaign_daily[
        "net_revenue"
    ]
    / channel_campaign_daily[
        "campaign_spend"
    ],

    np.nan
)


channel_campaign_daily[
    "conversion_rate"
] = np.where(
    channel_campaign_daily[
        "total_sessions"
    ].gt(0),

    channel_campaign_daily[
        "attributed_orders"
    ]
    / channel_campaign_daily[
        "total_sessions"
    ],

    np.nan
)


channel_campaign_daily[
    "pipeline_run_id"
] = pipeline_run_id


CHANNEL_CAMPAIGN_COLUMNS = [
    "metric_date",
    "sales_channel",
    "campaign_id",
    "campaign_spend",
    "attributed_orders",
    "attributed_customers",
    "gross_revenue",
    "net_revenue",
    "refunds",
    "roas",
    "conversion_rate",
    "pipeline_run_id"
]


channel_campaign_daily = (
    channel_campaign_daily[
        CHANNEL_CAMPAIGN_COLUMNS
    ].copy()
)


print(
    "channel_campaign_daily rows:",
    len(channel_campaign_daily)
)

print(
    channel_campaign_daily
    .head()
    .to_string(
        index=False
    )
)


# ============================================================
# 9. BUILD GOLD.EXECUTIVE_KPIS_DAILY
# ============================================================


exec_orders = (
    order_360
    .groupby(
        "order_date",
        as_index=False
    )
    .agg(
        total_orders=(
            "order_id",
            "nunique"
        ),
        gross_revenue=(
            "gross_merchandise_value",
            "sum"
        ),
        net_revenue=(
            "net_revenue",
            "sum"
        ),
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


exec_orders[
    "average_order_value"
] = np.where(
    exec_orders[
        "total_orders"
    ].gt(0),

    exec_orders[
        "net_revenue"
    ]
    / exec_orders[
        "total_orders"
    ],

    0.0
)


# ------------------------------------------------------------
# REFUND RATE
# ------------------------------------------------------------

refund_order_ids = set(
    completed_refund[
        "order_id"
    ].dropna()
)


captured_order_ids = set(
    captured[
        "order_id"
    ].dropna()
)


refund_rate_base = order_360[
    [
        "order_id",
        "order_date"
    ]
].copy()


refund_rate_base[
    "refunded_order"
] = refund_rate_base[
    "order_id"
].isin(
    refund_order_ids
)


refund_rate_base[
    "captured_order"
] = refund_rate_base[
    "order_id"
].isin(
    captured_order_ids
)


refund_daily = (
    refund_rate_base
    .groupby(
        "order_date",
        as_index=False
    )
    .agg(
        refunded_orders=(
            "refunded_order",
            "sum"
        ),
        captured_orders=(
            "captured_order",
            "sum"
        ),
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


refund_daily[
    "refund_rate"
] = np.where(
    refund_daily[
        "captured_orders"
    ].gt(0),

    refund_daily[
        "refunded_orders"
    ]
    / refund_daily[
        "captured_orders"
    ],

    0.0
)


# ------------------------------------------------------------
# RETURN RATE
# ------------------------------------------------------------

returned_order_ids = set(
    return_state[
        "order_id"
    ].dropna()
)


return_base = order_360[
    [
        "order_id",
        "order_date"
    ]
].copy()


return_base[
    "returned_order"
] = return_base[
    "order_id"
].isin(
    returned_order_ids
)


return_daily = (
    return_base
    .groupby(
        "order_date",
        as_index=False
    )
    .agg(
        returned_orders=(
            "returned_order",
            "sum"
        ),
        resolved_orders=(
            "order_id",
            "nunique"
        )
    )
    .rename(
        columns={
            "order_date":
                "metric_date"
        }
    )
)


return_daily[
    "return_rate"
] = np.where(
    return_daily[
        "resolved_orders"
    ].gt(0),

    return_daily[
        "returned_orders"
    ]
    / return_daily[
        "resolved_orders"
    ],

    0.0
)


# ------------------------------------------------------------
# CUSTOMER EXECUTIVE METRICS
# ------------------------------------------------------------

customer_exec = (
    customer_daily
    .groupby(
        "metric_date",
        as_index=False
    )
    .agg(
        active_customers=(
            "customer_id",
            "nunique"
        ),
        repeat_customers=(
            "repeat_customer_flag",
            "sum"
        ),
        customers_with_support=(
            "support_contacts",
            lambda s:
                int(
                    (s > 0).sum()
                )
        ),
    )
)


customer_exec[
    "repeat_customer_rate"
] = np.where(
    customer_exec[
        "active_customers"
    ].gt(0),

    customer_exec[
        "repeat_customers"
    ]
    / customer_exec[
        "active_customers"
    ],

    0.0
)


customer_exec[
    "support_contact_rate"
] = np.where(
    customer_exec[
        "active_customers"
    ].gt(0),

    customer_exec[
        "customers_with_support"
    ]
    / customer_exec[
        "active_customers"
    ],

    0.0
)


# ------------------------------------------------------------
# STOCKOUT RATE
# ------------------------------------------------------------

stock_exec = (
    product_daily[
        product_daily[
            "stockout_flag"
        ].notna()
    ]
    .groupby(
        "metric_date",
        as_index=False
    )
    .agg(
        stockout_products=(
            "stockout_flag",
            "sum"
        ),
        observed_products=(
            "product_id",
            "nunique"
        ),
    )
)


stock_exec[
    "stockout_rate"
] = np.where(
    stock_exec[
        "observed_products"
    ].gt(0),

    stock_exec[
        "stockout_products"
    ]
    / stock_exec[
        "observed_products"
    ],

    0.0
)


# ------------------------------------------------------------
# BUILD EXECUTIVE KPI
# ------------------------------------------------------------

executive_kpis_daily = (
    exec_orders
    .merge(
        refund_daily[
            [
                "metric_date",
                "refund_rate"
            ]
        ],
        on="metric_date",
        how="left"
    )
    .merge(
        return_daily[
            [
                "metric_date",
                "return_rate"
            ]
        ],
        on="metric_date",
        how="left"
    )
    .merge(
        customer_exec[
            [
                "metric_date",
                "repeat_customer_rate",
                "active_customers",
                "support_contact_rate"
            ]
        ],
        on="metric_date",
        how="left"
    )
    .merge(
        stock_exec[
            [
                "metric_date",
                "stockout_rate"
            ]
        ],
        on="metric_date",
        how="left"
    )
)


for col in [
    "refund_rate",
    "return_rate",
    "repeat_customer_rate",
    "stockout_rate",
    "support_contact_rate"
]:

    executive_kpis_daily[col] = (
        executive_kpis_daily[col]
        .fillna(0.0)
    )


executive_kpis_daily[
    "active_customers"
] = (
    executive_kpis_daily[
        "active_customers"
    ]
    .fillna(0)
    .astype("int64")
)


# ------------------------------------------------------------
# DATA FRESHNESS
# ------------------------------------------------------------

freshness_candidates = []


for df, col in [
    (
        orders,
        "updated_at_utc"
    ),
    (
        payment_events,
        "ingested_at_utc"
    ),
    (
        refund_events,
        "ingested_at_utc"
    ),
    (
        return_events,
        "ingested_at_utc"
    ),
    (
        support_events,
        "ingested_at_utc"
    ),
    (
        web_events,
        "ingested_at_utc"
    ),
]:

    if (
        col in df.columns
        and not df.empty
    ):

        freshness_candidates.append(
            pd.to_datetime(
                df[col],
                utc=True
            ).max()
        )


data_freshness_utc = max(
    freshness_candidates
)


executive_kpis_daily[
    "data_freshness_utc"
] = data_freshness_utc


executive_kpis_daily[
    "pipeline_run_id"
] = pipeline_run_id


EXECUTIVE_COLUMNS = [
    "metric_date",
    "total_orders",
    "gross_revenue",
    "net_revenue",
    "average_order_value",
    "refund_rate",
    "return_rate",
    "repeat_customer_rate",
    "stockout_rate",
    "active_customers",
    "support_contact_rate",
    "data_freshness_utc",
    "pipeline_run_id"
]


executive_kpis_daily = (
    executive_kpis_daily[
        EXECUTIVE_COLUMNS
    ].copy()
)


print(
    "executive_kpis_daily rows:",
    len(executive_kpis_daily)
)

print(
    executive_kpis_daily
    .head()
    .to_string(
        index=False
    )
)


# ============================================================
# DEBUG PRODUCT NET REVENUE RECONCILIATION
# ============================================================

order_product_check = (
    product_lines
    .groupby(
        "order_id",
        as_index=False
    )
    .agg(
        product_net_revenue=(
            "line_net_revenue",
            "sum"
        )
    )
)


order_product_check = (
    order_360[
        [
            "order_id",
            "gross_merchandise_value",
            "discount_amount",
            "shipping_revenue",
            "refunded_amount",
            "net_revenue"
        ]
    ]
    .merge(
        order_product_check,
        on="order_id",
        how="left"
    )
)


order_product_check[
    "product_net_revenue"
] = (
    order_product_check[
        "product_net_revenue"
    ].fillna(0)
)


order_product_check[
    "difference"
] = (
    order_product_check[
        "net_revenue"
    ]
    - order_product_check[
        "product_net_revenue"
    ]
)


problem_orders = (
    order_product_check[
        order_product_check[
            "difference"
        ].abs() > 0.0001
    ].copy()
)


print(
    "\nJumlah order bermasalah:",
    len(problem_orders)
)


print(
    "Total difference:",
    problem_orders[
        "difference"
    ].sum()
)


if not problem_orders.empty:

    print(
        problem_orders
        .sort_values(
            "difference",
            key=abs,
            ascending=False
        )
        .to_string(
            index=False
        )
    )


# ============================================================
# 10. GOLD QUALITY CHECKS DAN RECONCILIATION
# ============================================================

quality_results = []


def check(
    name,
    actual,
    expected,
    passed
):

    quality_results.append({
        "check_name":
            name,

        "actual":
            actual,

        "expected":
            expected,

        "status":
            "PASS"
            if passed
            else "FAIL",
    })


# ------------------------------------------------------------
# GRAIN UNIQUENESS
# ------------------------------------------------------------

checks_unique = [
    (
        "order_360 grain",
        order_360,
        ["order_id"]
    ),
    (
        "customer_daily grain",
        customer_daily,
        [
            "customer_id",
            "metric_date"
        ]
    ),
    (
        "product_daily grain",
        product_daily,
        [
            "product_id",
            "metric_date"
        ]
    ),
    (
        "channel_campaign_daily grain",
        channel_campaign_daily,
        [
            "metric_date",
            "sales_channel",
            "campaign_id"
        ]
    ),
    (
        "executive_kpis_daily grain",
        executive_kpis_daily,
        ["metric_date"]
    ),
]


for name, df, keys_ in checks_unique:

    duplicates = int(
        df.duplicated(
            keys_
        ).sum()
    )

    check(
        name,
        duplicates,
        0,
        duplicates == 0
    )


# ------------------------------------------------------------
# SILVER ORDERS -> GOLD ORDER 360
# ------------------------------------------------------------

check(
    "Silver orders -> Gold order_360 row count",
    len(order_360),
    len(orders),
    len(order_360) == len(orders)
)


# ------------------------------------------------------------
# ORDER 360 NET REVENUE FORMULA
# ------------------------------------------------------------

expected_net = (
    order_360[
        "gross_merchandise_value"
    ]
    - order_360[
        "discount_amount"
    ]
    + order_360[
        "shipping_revenue"
    ]
    - order_360[
        "refunded_amount"
    ]
)


net_diff = float(
    (
        order_360[
            "net_revenue"
        ]
        - expected_net
    )
    .abs()
    .max()
)


check(
    "order_360 net_revenue formula",
    round(
        net_diff,
        8
    ),
    0,
    net_diff < 1e-8
)


# ------------------------------------------------------------
# PRODUCT NET REVENUE RECONCILIATION
# ------------------------------------------------------------
#
# product_daily hanya dapat merekonsiliasi
# order yang memiliki valid product lines di Silver.
# ------------------------------------------------------------

orders_with_products = (
    product_lines[
        "order_id"
    ]
    .dropna()
    .unique()
)


expected_product_net_revenue = (
    order_360[
        order_360[
            "order_id"
        ].isin(
            orders_with_products
        )
    ][
        "net_revenue"
    ]
    .sum()
)


actual_product_net_revenue = (
    product_daily[
        "net_revenue"
    ].sum()
)


product_net_diff = abs(
    actual_product_net_revenue
    - expected_product_net_revenue
)


print(
    "\nProduct net revenue       :",
    actual_product_net_revenue
)


print(
    "Expected from valid orders:",
    expected_product_net_revenue
)


print(
    "Difference                :",
    product_net_diff
)


# ============================================================
# PENTING:
# Notebook sumber menghitung product_net_diff,
# tetapi tidak memanggil check(...) untuk nilai ini.
# Bagian ini dipertahankan sesuai notebook sumber.
# ============================================================


# ------------------------------------------------------------
# EXECUTIVE RECONCILIATION
# ------------------------------------------------------------

exec_orders_diff = abs(
    executive_kpis_daily[
        "total_orders"
    ].sum()
    - order_360[
        "order_id"
    ].nunique()
)


exec_net_diff = abs(
    executive_kpis_daily[
        "net_revenue"
    ].sum()
    - order_360[
        "net_revenue"
    ].sum()
)


check(
    "executive order reconciliation",
    int(exec_orders_diff),
    0,
    exec_orders_diff == 0
)


check(
    "executive net revenue reconciliation",
    round(
        float(
            exec_net_diff
        ),
        6
    ),
    0,
    exec_net_diff < 0.01
)


# ------------------------------------------------------------
# MANDATORY NULLS
# ------------------------------------------------------------

mandatory_nulls = {

    "order_360":
        int(
            order_360
            .drop(
                columns=[
                    "store_id"
                ]
            )
            .isna()
            .sum()
            .sum()
        ),

    "customer_daily":
        int(
            customer_daily
            .drop(
                columns=[
                    "active_channel"
                ]
            )
            .isna()
            .sum()
            .sum()
        ),

    "channel_campaign_daily":
        int(
            channel_campaign_daily
            .drop(
                columns=[
                    "roas",
                    "conversion_rate"
                ]
            )
            .isna()
            .sum()
            .sum()
        ),

    "executive_kpis_daily":
        int(
            executive_kpis_daily
            .isna()
            .sum()
            .sum()
        ),
}


for (
    table_name,
    null_count
) in mandatory_nulls.items():

    check(
        f"{table_name} mandatory nulls",
        null_count,
        0,
        null_count == 0
    )


quality_report = pd.DataFrame(
    quality_results
)


print()
print("GOLD QUALITY REPORT")
print(
    quality_report.to_string(
        index=False
    )
)


failed = quality_report[
    quality_report[
        "status"
    ].eq(
        "FAIL"
    )
]


if not failed.empty:

    raise RuntimeError(
        "Gold quality gate gagal: "
        + str(
            failed[
                "check_name"
            ].tolist()
        )
    )


print(
    "GOLD QUALITY GATE: PASS"
)


# ------------------------------------------------------------
# RECONCILIATION
# ------------------------------------------------------------

reconciliation = pd.DataFrame([
    {
        "metric":
            "silver_orders",

        "value":
            len(orders)
    },
    {
        "metric":
            "gold_order_360",

        "value":
            len(order_360)
    },
    {
        "metric":
            "order_360_total_gmv",

        "value":
            round(
                order_360[
                    "gross_merchandise_value"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "product_daily_total_gross",

        "value":
            round(
                product_daily[
                    "gross_revenue"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "order_360_total_net_revenue",

        "value":
            round(
                order_360[
                    "net_revenue"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "product_daily_total_net_revenue",

        "value":
            round(
                product_daily[
                    "net_revenue"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "executive_total_net_revenue",

        "value":
            round(
                executive_kpis_daily[
                    "net_revenue"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "completed_refund_amount",

        "value":
            round(
                completed_refund[
                    "amount"
                ].sum(),
                2
            )
    },
    {
        "metric":
            "order_360_refunded_amount",

        "value":
            round(
                order_360[
                    "refunded_amount"
                ].sum(),
                2
            )
    },
])


print()
print("RECONCILIATION")
print(
    reconciliation.to_string(
        index=False
    )
)


# ============================================================
# 11. CREATE GOLD SCHEMA
# ============================================================

GOLD_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.order_360 (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date DATE NOT NULL,
    sales_channel TEXT NOT NULL,
    store_id TEXT,
    gross_merchandise_value NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(14, 2) NOT NULL,
    shipping_revenue NUMERIC(14, 2) NOT NULL,
    captured_payment_amount NUMERIC(14, 2) NOT NULL,
    refunded_amount NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    item_count INTEGER NOT NULL,
    unit_quantity INTEGER NOT NULL,
    order_status TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    return_status TEXT NOT NULL,
    promotion_count INTEGER NOT NULL,
    first_order_flag BOOLEAN NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.customer_daily (
    customer_id TEXT NOT NULL,
    metric_date DATE NOT NULL,
    order_count INTEGER NOT NULL,
    unit_quantity INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    return_count INTEGER NOT NULL,
    support_contacts INTEGER NOT NULL,
    active_channel TEXT,
    new_customer_flag BOOLEAN NOT NULL,
    repeat_customer_flag BOOLEAN NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (customer_id, metric_date)
);

CREATE TABLE IF NOT EXISTS gold.product_daily (
    product_id TEXT NOT NULL,
    metric_date DATE NOT NULL,
    active_category TEXT,
    units_sold INTEGER NOT NULL,
    order_count INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    refunded_units INTEGER NOT NULL,
    available_inventory INTEGER,
    reserved_inventory INTEGER,
    stockout_flag BOOLEAN,
    promotion_count INTEGER NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, metric_date)
);

CREATE TABLE IF NOT EXISTS gold.channel_campaign_daily (
    metric_date DATE NOT NULL,
    sales_channel TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    campaign_spend NUMERIC(14, 2) NOT NULL,
    attributed_orders INTEGER NOT NULL,
    attributed_customers INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    refunds NUMERIC(14, 2) NOT NULL,
    roas NUMERIC(14, 4),
    conversion_rate NUMERIC(14, 4),
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (
        metric_date,
        sales_channel,
        campaign_id
    )
);

CREATE TABLE IF NOT EXISTS gold.executive_kpis_daily (
    metric_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    average_order_value NUMERIC(14, 2) NOT NULL,
    refund_rate NUMERIC(14, 4) NOT NULL,
    return_rate NUMERIC(14, 4) NOT NULL,
    repeat_customer_rate NUMERIC(14, 4) NOT NULL,
    stockout_rate NUMERIC(14, 4) NOT NULL,
    active_customers INTEGER NOT NULL,
    support_contact_rate NUMERIC(14, 4) NOT NULL,
    data_freshness_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL
);
"""


with engine.begin() as connection:

    for statement in [
        s.strip()
        for s in GOLD_DDL.split(";")
        if s.strip()
    ]:

        connection.execute(
            text(statement)
        )


print(
    "Gold schema contract ready."
)


# ============================================================
# 12. WRITE GOLD
# ============================================================

gold_tables = {

    "order_360":
        order_360,

    "customer_daily":
        customer_daily,

    "product_daily":
        product_daily,

    "channel_campaign_daily":
        channel_campaign_daily,

    "executive_kpis_daily":
        executive_kpis_daily,
}


# ------------------------------------------------------------
# RERUNNABLE:
# kosongkan Gold lalu insert hasil terbaru
# ------------------------------------------------------------

with engine.begin() as connection:

    for table_name in [
        "channel_campaign_daily",
        "product_daily",
        "customer_daily",
        "executive_kpis_daily",
        "order_360"
    ]:

        connection.execute(
            text(
                f"TRUNCATE TABLE "
                f"gold.{table_name}"
            )
        )


for (
    table_name,
    df
) in gold_tables.items():

    df.to_sql(
        table_name,
        con=engine,
        schema="gold",
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )

    print(
        f"[GOLD] "
        f"{table_name}: "
        f"{len(df):,} rows"
    )


print()
print(
    "Gold layer berhasil disimpan."
)


# ============================================================
# 13. VALIDATE GOLD WRITE
# ============================================================

stored_counts = pd.read_sql(
    text("""
        SELECT
            'order_360' AS table_name,
            COUNT(*) AS row_count
        FROM gold.order_360

        UNION ALL

        SELECT
            'customer_daily',
            COUNT(*)
        FROM gold.customer_daily

        UNION ALL

        SELECT
            'product_daily',
            COUNT(*)
        FROM gold.product_daily

        UNION ALL

        SELECT
            'channel_campaign_daily',
            COUNT(*)
        FROM gold.channel_campaign_daily

        UNION ALL

        SELECT
            'executive_kpis_daily',
            COUNT(*)
        FROM gold.executive_kpis_daily

        ORDER BY table_name
    """),
    engine
)


print()
print(
    stored_counts.to_string(
        index=False
    )
)


expected_counts = {
    name: len(df)
    for name, df
    in gold_tables.items()
}


for row in stored_counts.itertuples(
    index=False
):

    assert (
        int(row.row_count)
        == expected_counts[
            row.table_name
        ]
    ), (
        "Row count mismatch: "
        f"{row.table_name}"
    )


print(
    "GOLD WRITE VALIDATION: PASS"
)


# ============================================================
# 14. PERTANYAAN ANALITIK PADA GOLD
# ============================================================

analytics_queries = {

    "Q01_total_net_revenue_by_channel":
    """
        SELECT
            sales_channel,
            ROUND(
                SUM(net_revenue),
                2
            ) AS total_net_revenue
        FROM gold.order_360
        GROUP BY sales_channel
        ORDER BY total_net_revenue DESC
    """,

    "Q03_orders_each_day":
    """
        SELECT
            metric_date,
            total_orders
        FROM gold.executive_kpis_daily
        ORDER BY metric_date
    """,

    "Q05_products_with_stockout":
    """
        SELECT
            product_id,
            metric_date,
            active_category,
            available_inventory
        FROM gold.product_daily
        WHERE stockout_flag = TRUE
        ORDER BY
            metric_date,
            product_id
        LIMIT 50
    """,

    "Q09_campaign_roas":
    """
        SELECT
            campaign_id,
            ROUND(
                SUM(net_revenue)
                / NULLIF(
                    SUM(campaign_spend),
                    0
                ),
                4
            ) AS roas
        FROM gold.channel_campaign_daily
        GROUP BY campaign_id
        HAVING SUM(campaign_spend) > 0
        ORDER BY roas DESC
    """,

    "Q11_daily_net_revenue":
    """
        SELECT
            metric_date,
            net_revenue
        FROM gold.executive_kpis_daily
        ORDER BY metric_date
    """,
}


for (
    question_id,
    sql
) in analytics_queries.items():

    print()
    print(
        "=" * 70
    )
    print(
        question_id
    )

    query_result = pd.read_sql(
        text(sql),
        engine
    )

    print(
        query_result
        .head(20)
        .to_string(
            index=False
        )
    )


# ============================================================
# 15. JALANKAN PERTANYAAN MELALUI ENGINE
# ============================================================

import requests


ENGINE_URL = os.getenv(
    "ENGINE_URL",
    "http://localhost:8000"
)


engine_questions = [
    "What is total net revenue by sales channel?",
    "Which products had stockouts?",
    "What was the daily refund rate?",
    "Which campaigns had the best roas?",
]


try:

    health = requests.get(
        f"{ENGINE_URL}/health",
        timeout=3
    )

    health.raise_for_status()

    print(
        "Engine health:",
        health.json()
    )


    for question in engine_questions:

        response = requests.post(
            f"{ENGINE_URL}/v1/analyze",
            json={
                "question":
                    question
            },
            timeout=60,
        )

        response.raise_for_status()

        result = response.json()

        print()
        print(
            "Question:",
            question
        )

        print(
            "SQL:",
            result.get(
                "sql"
            )
        )

        print(
            "Explanation:",
            result.get(
                "explanation"
            )
        )


        engine_rows = pd.DataFrame(
            result.get(
                "rows",
                []
            )
        )


        if not engine_rows.empty:

            print(
                engine_rows
                .head(20)
                .to_string(
                    index=False
                )
            )

        else:

            print(
                "No rows returned."
            )


except requests.RequestException as exc:

    print(
        "Engine belum aktif:",
        exc
    )

    print(
        "Jalankan engine dengan "
        "ANALYTICS_DB_TARGET=local, "
        "lalu jalankan kembali bagian engine."
    )


# ============================================================
# 16. FINAL GOLD SUMMARY
# ============================================================

print()
print(
    "=" * 60
)

print(
    "FINAL GOLD QUALITY GATE"
)

print(
    "=" * 60
)


print(
    "order_360 rows              :",
    len(order_360)
)

print(
    "customer_daily rows         :",
    len(customer_daily)
)

print(
    "product_daily rows          :",
    len(product_daily)
)

print(
    "channel_campaign_daily rows :",
    len(channel_campaign_daily)
)

print(
    "executive_kpis_daily rows   :",
    len(executive_kpis_daily)
)


print(
    "=" * 60
)

print(
    "GOLD QUALITY GATE: PASS"
)

print(
    "Gold layer siap digunakan "
    "oleh NL-to-SQL engine."
)


# ============================================================
# SELESAI
# ============================================================

engine.dispose()