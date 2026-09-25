"""Participant-owned Airflow DAG for the Bronze -> Silver -> Gold pipeline.

This file is a starter orchestration contract. Participants may improve the
operators, retries, and alerting strategy, but the DAG must retain the layer
boundaries and quality gate described in README-id.md.
"""

#from __future__ import annotations

#import json
#import os
#from datetime import UTC, datetime
#from pathlib import Path

#from airflow import DAG
#from airflow.exceptions import AirflowException
#from airflow.operators.python import PythonOperator

#from pipelines.ingestion.db import connect
#from pipelines.ingestion.initialize_database import initialize_database
#from pipelines.quality.checks import hard_failures, record_quality_results, run_quality_checks

#PROJECT_ROOT = Path(__file__).resolve().parents[1]
#RAW_ROOT = Path(os.getenv("RAW_DATA_PATH", str(PROJECT_ROOT / "data" / "raw")))


#def validate_raw_snapshot() -> None:
 #   """Fail early when the Google Drive snapshot is incomplete."""

#    required_paths = (
 #       RAW_ROOT / "manifest.json",
  #      RAW_ROOT / "operational",
   #     RAW_ROOT / "events",
    #    RAW_ROOT / "inventory",
     #   RAW_ROOT / "reference",
    #)
    #missing = [str(path) for path in required_paths if not path.exists()]
    #if missing:
     #   raise AirflowException(f"Raw data lake snapshot is incomplete: {missing}")

    #manifest = json.loads((RAW_ROOT / "manifest.json").read_text(encoding="utf-8"))
    #for key in ("run_id", "seed", "start_date", "end_date", "files"):
     #   if key not in manifest:
      #      raise AirflowException(f"Manifest is missing required field: {key}")


#def initialize_local_schemas() -> None:
 #   """Create the participant's local Bronze, Silver, Gold, and Ops schemas."""

  #  initialize_database(target="local")


#def load_bronze_snapshot() -> str:
 #   """Load the raw snapshot and return its stable pipeline run identifier."""

  #  from pipelines.bronze.build_bronze import load_bronze

   # return load_bronze(RAW_ROOT, target="local")


#def start_pipeline_run(**context) -> None:
 #   """Create the operations record after Bronze has produced a run id."""

  #  run_id = context["ti"].xcom_pull(task_ids="load_bronze_snapshot")
   # if not run_id:
    #    raise AirflowException("Bronze did not return a pipeline run id")

    #with connect("local") as connection:
     #   connection.execute(
      #      """
       #     INSERT INTO ops.pipeline_runs
        #        (run_id, started_at_utc, status, current_stage)
         #   VALUES (%s, %s, 'RUNNING', 'silver')
          #  ON CONFLICT (run_id) DO UPDATE SET
           #     started_at_utc = EXCLUDED.started_at_utc,
            #    completed_at_utc = NULL,
             #   status = 'RUNNING',
              #  current_stage = 'silver',
               # error_message = NULL
           # """,
            #(run_id, datetime.now(UTC)),
        #)
        #connection.commit()


#def build_silver_layer(**context) -> None:
 #   """Run the participant Silver transformation for the current run."""

  #  from pipelines.silver.build_silver import build_silver

    #run_id = context["ti"].xcom_pull(task_ids="load_bronze_snapshot")
    #build_silver(run_id, mode="student", target="local")


#def silver_quality_gate(**context) -> None:
 #   """Stop the DAG before Gold when Silver has no usable order entities."""

    #run_id = context["ti"].xcom_pull(task_ids="load_bronze_snapshot")
    #with connect("local") as connection:
        #count = connection.execute(
         #   "SELECT COUNT(*) FROM silver.orders WHERE pipeline_run_id = %s",
          #  (run_id,),
        #).fetchone()[0]
    #if count == 0:
     #   raise AirflowException(f"Silver quality gate failed for {run_id}: no orders were produced")


#def build_gold_layer(**context) -> None:
 #   """Execute the participant Gold SQL after the Silver quality gate."""

  #  run_id = context["ti"].xcom_pull(task_ids="load_bronze_snapshot")
   # sql = (PROJECT_ROOT / "pipelines" / "gold" / "build_gold.sql").read_text(encoding="utf-8")
    #with connect("local") as connection:
     #   connection.execute("SELECT set_config('app.pipeline_run_id', %s, false)", (run_id,))
      #  connection.execute(sql)
       # connection.commit()


#def validate_gold_layer(**context) -> None:
 #   """Record final quality results and fail the DAG on hard rule failures."""

  #  run_id = context["ti"].xcom_pull(task_ids="load_bronze_snapshot")
   # with connect("local") as connection:
       # connection.execute("SELECT set_config('app.pipeline_run_id', %s, false)", (run_id,))
        #results = run_quality_checks(connection)
        #record_quality_results(connection, run_id, results)
        #failures = hard_failures(results)
        #status = "FAILED" if failures else "SUCCEEDED"
        #connection.execute(
         #   """
          #  UPDATE ops.pipeline_runs
           # SET completed_at_utc = %s,
            #    status = %s,
            #    current_stage = 'quality',
             #   quality_rule_failures = %s
           # WHERE run_id = %s
           # """,
           # (datetime.now(UTC), status, len(failures), run_id),
       # )
        #connection.commit()
    #if failures:
       # names = ", ".join(result.rule_name for result in failures)
       # raise AirflowException(f"Gold quality gate failed for {run_id}: {names}")


#with DAG(
   # dag_id="p1m1_omnichannel_retail_nl2sql",
   # start_date=datetime(2026, 1, 1, tzinfo=UTC),
    #schedule=None,
    #catchup=False,
    #default_args={"owner": "participant", "retries": 1},
    #tags=["p1m1", "retail", "bronze-silver-gold"],
#) as dag:
   # validate_input = PythonOperator(
       # task_id="validate_raw_snapshot",
      #  python_callable=validate_raw_snapshot,
    #)
    #initialize_database_task = PythonOperator(
      #  task_id="initialize_local_schemas",
       # python_callable=initialize_local_schemas,
    #)
    #load_bronze_task = PythonOperator(
       # task_id="load_bronze_snapshot",
        #python_callable=load_bronze_snapshot,
    #)
    #start_run_task = PythonOperator(
       # task_id="start_pipeline_run",
       # python_callable=start_pipeline_run,
    #)
    #silver_task = PythonOperator(
       # task_id="build_silver_layer",
       # python_callable=build_silver_layer,
    #)
    #silver_gate_task = PythonOperator(
      #  task_id="silver_quality_gate",
      #  python_callable=silver_quality_gate,
    #)
    #gold_task = PythonOperator(
      #  task_id="build_gold_layer",
      #  python_callable=build_gold_layer,
    #)
    #gold_validation_task = PythonOperator(
       # task_id="validate_gold_layer",
       # python_callable=validate_gold_layer,
    #)

   # (
       # validate_input
    #     # >> initialize_database_task
    #     >> load_bronze_task
    #     >> start_run_task
    #     >> silver_task
    #     >> silver_gate_task
    #     >> gold_task
    #     >> gold_validation_task
    # )



# AIRFLOW DAG — OMNICHANNEL RETAIL PIPELINE
# Pipeline:
# validate manifest dan raw files
#             ↓
# initialize PostgreSQL schemas
#             ↓
# load Bronze
#             ↓
# build Silver
#             ↓
# quality gate
#             ↓
# build Gold
#             ↓
# validate Gold dan record pipeline metadata
#
# Target:
# PostgreSQL LOCAL
#
# Bronze dan Silver TIDAK dikirim ke Neon.


# 1. IMPORTS

import os
import sys
import uuid
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL



# 2. PROJECT CONFIGURATION

DAG_FILE = Path(__file__).resolve()

PROJECT_ROOT = DAG_FILE.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

PIPELINES_DIR = PROJECT_ROOT / "pipelines"

BRONZE_SCRIPT = PIPELINES_DIR / "bronze" / "build_bronze.py"
SILVER_SCRIPT = PIPELINES_DIR / "silver" / "build_silver.py"
GOLD_SCRIPT = PIPELINES_DIR / "gold" / "build_gold.py"


# ============================================================
# 3. LOCAL POSTGRESQL CONFIGURATION
# ============================================================
#
# Pipeline utama wajib menggunakan PostgreSQL lokal.
#
# Tidak menggunakan Neon untuk Bronze maupun Silver.
# ============================================================

DB_USER = os.getenv(
    "DB_USER",
    "postgres"
)

DB_PASSWORD = os.getenv(
    "DB_PASSWORD",
    "postgres"
)

DB_HOST = os.getenv(
    "DB_HOST",
    "localhost"
)

DB_PORT = int(
    os.getenv(
        "DB_PORT",
        "5432"
    )
)

DB_NAME = os.getenv(
    "DB_NAME",
    "milestone1_omnichannel"
)


DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)


def get_engine():

    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
    )


# ============================================================
# 4. REQUIRED RAW FILES
# ============================================================
#
# File ini mengikuti source yang digunakan oleh Bronze/Silver.
# ============================================================

EXPECTED_FILES = [

    "city_reference.json",
    "sales_channels.json",
    "products.json",
    "stores.json",
    "promotions.json",

    "customers.json",
    "customer_profiles.json",
    "customer_addresses.json",
    "product_categories.json",

    "orders.json",
    "order_items.json",
    "order_promotions.json",

    "payment_events.json",
    "refund_events.json",
    "return_events.json",
    "support_events.json",
    "web_events.json",

    "inventory_snapshots.csv",
    "campaign_spend.csv",
]


# ============================================================
# 5. DEFAULT DAG ARGUMENTS
# ============================================================

default_args = {

    "owner":
        "data-engineering",

    "depends_on_past":
        False,

    "retries":
        0,

    "retry_delay":
        timedelta(
            minutes=1
        ),
}


# ============================================================
# 6. HELPER — RECORD OPS STATUS
# ============================================================

def record_pipeline_status(
    pipeline_run_id,
    dag_run_id,
    status,
    stage,
    error_message=None,
):

    engine = get_engine()

    try:

        with engine.begin() as connection:

            connection.execute(
                text("""
                    CREATE SCHEMA
                    IF NOT EXISTS ops
                """)
            )

            connection.execute(
                text("""
                    CREATE TABLE
                    IF NOT EXISTS ops.pipeline_runs
                    (
                        pipeline_run_id TEXT PRIMARY KEY,
                        dag_run_id TEXT,
                        status TEXT NOT NULL,
                        current_stage TEXT,
                        started_at_utc TIMESTAMPTZ,
                        finished_at_utc TIMESTAMPTZ,
                        error_message TEXT,
                        updated_at_utc TIMESTAMPTZ
                            NOT NULL DEFAULT NOW()
                    )
                """)
            )

            connection.execute(
                text("""
                    INSERT INTO ops.pipeline_runs
                    (
                        pipeline_run_id,
                        dag_run_id,
                        status,
                        current_stage,
                        started_at_utc,
                        finished_at_utc,
                        error_message,
                        updated_at_utc
                    )
                    VALUES
                    (
                        :pipeline_run_id,
                        :dag_run_id,
                        :status,
                        :stage,

                        CASE
                            WHEN :status = 'RUNNING'
                            THEN NOW()
                            ELSE NULL
                        END,

                        CASE
                            WHEN :status IN
                            (
                                'SUCCESS',
                                'FAILED'
                            )
                            THEN NOW()
                            ELSE NULL
                        END,

                        :error_message,
                        NOW()
                    )

                    ON CONFLICT
                    (
                        pipeline_run_id
                    )

                    DO UPDATE SET

                        dag_run_id =
                            EXCLUDED.dag_run_id,

                        status =
                            EXCLUDED.status,

                        current_stage =
                            EXCLUDED.current_stage,

                        finished_at_utc =
                            CASE
                                WHEN EXCLUDED.status IN
                                (
                                    'SUCCESS',
                                    'FAILED'
                                )
                                THEN NOW()

                                ELSE
                                    ops.pipeline_runs.finished_at_utc
                            END,

                        error_message =
                            EXCLUDED.error_message,

                        updated_at_utc =
                            NOW()
                """),

                {
                    "pipeline_run_id":
                        pipeline_run_id,

                    "dag_run_id":
                        dag_run_id,

                    "status":
                        status,

                    "stage":
                        stage,

                    "error_message":
                        error_message,
                }
            )

    finally:

        engine.dispose()


# ============================================================
# 7. HELPER — RECORD STAGE STATUS
# ============================================================

def record_stage_status(
    pipeline_run_id,
    stage_name,
    status,
    error_message=None,
):

    engine = get_engine()

    try:

        with engine.begin() as connection:

            connection.execute(
                text("""
                    CREATE SCHEMA
                    IF NOT EXISTS ops
                """)
            )

            connection.execute(
                text("""
                    CREATE TABLE
                    IF NOT EXISTS ops.pipeline_stage_runs
                    (
                        pipeline_run_id TEXT NOT NULL,
                        stage_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        started_at_utc TIMESTAMPTZ,
                        finished_at_utc TIMESTAMPTZ,
                        error_message TEXT,
                        updated_at_utc TIMESTAMPTZ
                            NOT NULL DEFAULT NOW(),

                        PRIMARY KEY
                        (
                            pipeline_run_id,
                            stage_name
                        )
                    )
                """)
            )

            connection.execute(
                text("""
                    INSERT INTO ops.pipeline_stage_runs
                    (
                        pipeline_run_id,
                        stage_name,
                        status,
                        started_at_utc,
                        finished_at_utc,
                        error_message,
                        updated_at_utc
                    )

                    VALUES
                    (
                        :pipeline_run_id,
                        :stage_name,
                        :status,

                        CASE
                            WHEN :status = 'RUNNING'
                            THEN NOW()
                            ELSE NULL
                        END,

                        CASE
                            WHEN :status IN
                            (
                                'SUCCESS',
                                'FAILED'
                            )
                            THEN NOW()
                            ELSE NULL
                        END,

                        :error_message,
                        NOW()
                    )

                    ON CONFLICT
                    (
                        pipeline_run_id,
                        stage_name
                    )

                    DO UPDATE SET

                        status =
                            EXCLUDED.status,

                        finished_at_utc =
                            CASE
                                WHEN EXCLUDED.status IN
                                (
                                    'SUCCESS',
                                    'FAILED'
                                )
                                THEN NOW()

                                ELSE
                                    ops.pipeline_stage_runs.finished_at_utc
                            END,

                        error_message =
                            EXCLUDED.error_message,

                        updated_at_utc =
                            NOW()
                """),

                {
                    "pipeline_run_id":
                        pipeline_run_id,

                    "stage_name":
                        stage_name,

                    "status":
                        status,

                    "error_message":
                        error_message,
                }
            )

    finally:

        engine.dispose()


# ============================================================
# 8. HELPER — RUN PYTHON SCRIPT
# ============================================================

def run_pipeline_script(
    script_path,
    pipeline_run_id,
    stage_name,
):

    if not script_path.exists():

        raise AirflowException(
            f"Script tidak ditemukan: "
            f"{script_path}"
        )

    env = os.environ.copy()

    # --------------------------------------------------------
    # PIPELINE RUN ID YANG SAMA DIKIRIM KE SEMUA LAYER
    # --------------------------------------------------------

    env[
        "PIPELINE_RUN_ID"
    ] = pipeline_run_id


    # --------------------------------------------------------
    # PASTIKAN TARGET DATABASE LOCAL
    # --------------------------------------------------------

    env["DB_USER"] = DB_USER
    env["DB_PASSWORD"] = DB_PASSWORD
    env["DB_HOST"] = DB_HOST
    env["DB_PORT"] = str(DB_PORT)
    env["DB_NAME"] = DB_NAME

    # Jangan mengaktifkan SSL Neon
    env.pop(
        "DB_SSLMODE",
        None
    )


    record_stage_status(
        pipeline_run_id,
        stage_name,
        "RUNNING",
    )


    try:

        result = subprocess.run(
            [
                sys.executable,
                "-u",
                str(script_path),
            ],
            cwd=str(PROJECT_ROOT),
            env=env,
            text=True,
            check=False,
        )


        if result.returncode != 0:

            error_message = (
                f"{stage_name} failed with exit code "
                f"{result.returncode}"
            )


            record_stage_status(
                pipeline_run_id,
                stage_name,
                "FAILED",
                error_message,
            )
            raise AirflowException(error_message)


        record_stage_status(
            pipeline_run_id,
            stage_name,
            "SUCCESS",
        )


    except Exception as exc:

        record_stage_status(
            pipeline_run_id,
            stage_name,
            "FAILED",
            str(exc)[
                :5000
            ],
        )

        raise


# ============================================================
# 9. DAG DEFINITION
# ============================================================

with DAG(

    dag_id=
        "omnichannel_retail_pipeline",

    description=
        "Bronze Silver Gold omnichannel "
        "retail pipeline",

    default_args=
        default_args,

    start_date=
        datetime(
            2026,
            1,
            1
        ),

    schedule_interval='0 19 * * *',

    catchup=
        False,

    max_active_runs=
        1,

    tags=[
        "omnichannel",
        "bronze",
        "silver",
        "gold",
        "postgresql",
    ],

) as dag:


    # ========================================================
    # TASK 0 — GENERATE PIPELINE RUN ID
    # ========================================================

    @task
    def create_pipeline_run(
        **context
    ):

        # ----------------------------------------------------
        # Gunakan dag_run.run_id sebagai dasar deterministic.
        #
        # Artinya:
        # retry task dalam DAG run yang sama
        # menggunakan pipeline_run_id yang sama.
        # ----------------------------------------------------

        dag_run_id = (
            context[
                "dag_run"
            ].run_id
        )


        pipeline_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    "omnichannel-retail:"
                    + dag_run_id
                )
            )
        )


        record_pipeline_status(
            pipeline_run_id=
                pipeline_run_id,

            dag_run_id=
                dag_run_id,

            status=
                "RUNNING",

            stage=
                "pipeline_started",
        )


        print(
            "Pipeline Run ID:",
            pipeline_run_id
        )


        return pipeline_run_id


        # ========================================================
    # TASK 1 — VALIDATE RAW FILES
    # ========================================================

    @task
    def validate_raw_files(
        pipeline_run_id
    ):

        stage_name = (
            "validate_raw_files"
        )

        record_stage_status(
            pipeline_run_id,
            stage_name,
            "RUNNING",
        )

        try:

            if not RAW_DIR.exists():

                raise AirflowException(
                    "Raw directory tidak ditemukan: "
                    f"{RAW_DIR}"
                )

            # ------------------------------------------------
            # VALIDATE EXPECTED FILES
            # ------------------------------------------------

            raw_file_paths = {}

            for filename in EXPECTED_FILES:

                matches = list(
                    RAW_DIR.rglob(filename)
                )

                if matches:
                    raw_file_paths[filename] = matches[0]


            missing_files = [

                filename

                for filename in EXPECTED_FILES

                if filename not in raw_file_paths
            ]


            if missing_files:

                raise AirflowException(
                    "Raw files tidak lengkap. "
                    "Missing: "
                    + ", ".join(
                        missing_files
                    )
                )


            # ------------------------------------------------
            # VALIDATE FILE TIDAK KOSONG
            # ------------------------------------------------

            empty_files = [

                filename

                for filename, path
                in raw_file_paths.items()

                if path.stat().st_size == 0
            ]


            if empty_files:

                raise AirflowException(
                    "Raw files kosong: "
                    + ", ".join(
                        empty_files
                    )
                )


            print(
                "Raw files: VALID"
            )


            record_stage_status(
                pipeline_run_id,
                stage_name,
                "SUCCESS",
            )


        except Exception as exc:

            record_stage_status(
                pipeline_run_id,
                stage_name,
                "FAILED",
                error_message=str(exc),
            )

            raise
    # ========================================================
    # TASK 2 — INITIALIZE POSTGRESQL SCHEMAS
    # ========================================================

    @task
    def initialize_postgresql_schemas(
        pipeline_run_id
    ):

        stage_name = (
            "initialize_postgresql_schemas"
        )


        record_stage_status(
            pipeline_run_id,
            stage_name,
            "RUNNING",
        )


        engine = get_engine()


        try:

            with engine.begin() as connection:

                # --------------------------------------------
                # VALIDATE LOCAL DATABASE
                # --------------------------------------------

                database_name = (
                    connection.execute(
                        text(
                            "SELECT current_database()"
                        )
                    ).scalar()
                )


                print(
                    "PostgreSQL database:",
                    database_name
                )


                # --------------------------------------------
                # INITIALIZE SCHEMAS
                # --------------------------------------------

                connection.execute(
                    text(
                        "CREATE SCHEMA "
                        "IF NOT EXISTS bronze"
                    )
                )


                connection.execute(
                    text(
                        "CREATE SCHEMA "
                        "IF NOT EXISTS silver"
                    )
                )


                connection.execute(
                    text(
                        "CREATE SCHEMA "
                        "IF NOT EXISTS gold"
                    )
                )


                connection.execute(
                    text(
                        "CREATE SCHEMA "
                        "IF NOT EXISTS ops"
                    )
                )


                # --------------------------------------------
                # OPS PIPELINE RUN TABLE
                # --------------------------------------------

                connection.execute(
                    text("""
                        CREATE TABLE
                        IF NOT EXISTS
                        ops.pipeline_runs
                        (
                            pipeline_run_id TEXT
                                PRIMARY KEY,

                            dag_run_id TEXT,

                            status TEXT
                                NOT NULL,

                            current_stage TEXT,

                            started_at_utc
                                TIMESTAMPTZ,

                            finished_at_utc
                                TIMESTAMPTZ,

                            error_message TEXT,

                            updated_at_utc
                                TIMESTAMPTZ
                                NOT NULL
                                DEFAULT NOW()
                        )
                    """)
                )


                # --------------------------------------------
                # OPS PIPELINE STAGE TABLE
                # --------------------------------------------

                connection.execute(
                    text("""
                        CREATE TABLE
                        IF NOT EXISTS
                        ops.pipeline_stage_runs
                        (
                            pipeline_run_id TEXT
                                NOT NULL,

                            stage_name TEXT
                                NOT NULL,

                            status TEXT
                                NOT NULL,

                            started_at_utc
                                TIMESTAMPTZ,

                            finished_at_utc
                                TIMESTAMPTZ,

                            error_message TEXT,

                            updated_at_utc
                                TIMESTAMPTZ
                                NOT NULL
                                DEFAULT NOW(),

                            PRIMARY KEY
                            (
                                pipeline_run_id,
                                stage_name
                            )
                        )
                    """)
                )


            record_pipeline_status(
                pipeline_run_id=
                    pipeline_run_id,

                dag_run_id=
                    None,

                status=
                    "RUNNING",

                stage=
                    stage_name,
            )


            record_stage_status(
                pipeline_run_id,
                stage_name,
                "SUCCESS",
            )


            print(
                "PostgreSQL schemas initialized."
            )


        except Exception as exc:

            record_stage_status(
                pipeline_run_id,
                stage_name,
                "FAILED",
                str(exc)[
                    :5000
                ],
            )

            raise


        finally:

            engine.dispose()


    # ========================================================
    # TASK 3 — LOAD BRONZE
    # ========================================================

    @task
    def load_bronze(
        pipeline_run_id
    ):

        record_pipeline_status(
            pipeline_run_id,
            None,
            "RUNNING",
            "load_bronze",
        )


        run_pipeline_script(
            BRONZE_SCRIPT,
            pipeline_run_id,
            "load_bronze",
        )


    # ========================================================
    # TASK 4 — BUILD SILVER
    # ========================================================

    @task
    def build_silver(
        pipeline_run_id
    ):

        record_pipeline_status(
            pipeline_run_id,
            None,
            "RUNNING",
            "build_silver",
        )


        run_pipeline_script(
            SILVER_SCRIPT,
            pipeline_run_id,
            "build_silver",
        )


    # ========================================================
    # TASK 5 — SILVER QUALITY GATE
    # ========================================================

    @task
    def silver_quality_gate(
        pipeline_run_id
    ):

        stage_name = (
            "silver_quality_gate"
        )


        record_pipeline_status(
            pipeline_run_id,
            None,
            "RUNNING",
            stage_name,
        )


        record_stage_status(
            pipeline_run_id,
            stage_name,
            "RUNNING",
        )


        engine = get_engine()


        try:

            with engine.connect() as connection:

                # --------------------------------------------
                # CHECK SILVER TABLES EXIST
                # --------------------------------------------

                required_tables = [
                    "orders",
                    "order_items",
                    "products",
                    "customers",
                    "payment_events",
                    "refund_events",
                    "return_events",
                    "support_events",
                    "web_events",
                    "inventory_snapshots",
                    "campaign_spend",
                    "rejected_records",
                ]


                existing_tables = {
                    row[0]

                    for row in connection.execute(
                        text("""
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'silver'
                        """)
                    )
                }


                missing_tables = [
                    table

                    for table
                    in required_tables

                    if table
                    not in existing_tables
                ]


                if missing_tables:

                    raise AirflowException(
                        "Silver tables missing: "
                        + ", ".join(
                            missing_tables
                        )
                    )


                # --------------------------------------------
                # DUPLICATE ORDER
                # --------------------------------------------

                duplicate_orders = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM
                            (
                                SELECT
                                    order_id
                                FROM silver.orders
                                GROUP BY order_id
                                HAVING COUNT(*) > 1
                            ) q
                        """)
                    ).scalar()
                )


                if duplicate_orders != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{duplicate_orders} "
                        "duplicate order_id."
                    )


                # --------------------------------------------
                # DUPLICATE ORDER ITEM
                # --------------------------------------------

                duplicate_items = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM
                            (
                                SELECT
                                    order_item_id
                                FROM silver.order_items
                                GROUP BY order_item_id
                                HAVING COUNT(*) > 1
                            ) q
                        """)
                    ).scalar()
                )


                if duplicate_items != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{duplicate_items} "
                        "duplicate order_item_id."
                    )


                # --------------------------------------------
                # INVALID QUANTITY
                # --------------------------------------------

                invalid_quantity = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.order_items
                            WHERE quantity <= 0
                        """)
                    ).scalar()
                )


                if invalid_quantity != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{invalid_quantity} "
                        "invalid quantity."
                    )


                # --------------------------------------------
                # ORPHAN ORDER -> CUSTOMER
                # --------------------------------------------

                orphan_customer = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.orders o
                            LEFT JOIN
                                silver.customers c
                                ON
                                o.customer_id =
                                c.customer_id
                            WHERE
                                c.customer_id
                                IS NULL
                        """)
                    ).scalar()
                )


                if orphan_customer != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{orphan_customer} "
                        "orphan customer."
                    )


                # ORPHAN ITEM -> ORDER
                orphan_order = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.order_items i
                            LEFT JOIN
                                silver.orders o
                                ON
                                i.order_id =
                                o.order_id
                            WHERE
                                o.order_id
                                IS NULL
                        """)
                    ).scalar()
                )


                if orphan_order != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{orphan_order} "
                        "orphan order item."
                    )

                # ORPHAN ITEM -> PRODUCT

                orphan_product = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.order_items i
                            LEFT JOIN
                                silver.products p
                                ON
                                i.product_id =
                                p.product_id
                            WHERE
                                p.product_id
                                IS NULL
                        """)
                    ).scalar()
                )


                if orphan_product != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{orphan_product} "
                        "orphan product."
                    )

                
                # LINEAGE CHECK

                lineage_null = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.orders
                            WHERE raw_record_id IS NULL
                               OR pipeline_run_id IS NULL
                               OR source_file IS NULL
                               OR source_row_number IS NULL
                               OR record_checksum IS NULL
                        """)
                    ).scalar()
                )


                if lineage_null != 0:

                    raise AirflowException(
                        "Silver quality gate gagal: "
                        f"{lineage_null} "
                        "orders tanpa lineage."
                    )


            record_stage_status(
                pipeline_run_id,
                stage_name,
                "SUCCESS",
            )


            print(
                "=" * 60
            )

            print(
                "SILVER QUALITY GATE: PASS"
            )

            print(
                "=" * 60
            )


        except Exception as exc:

            record_stage_status(
                pipeline_run_id,
                stage_name,
                "FAILED",
                str(exc)[
                    :5000
                ],
            )


            record_pipeline_status(
                pipeline_run_id,
                None,
                "FAILED",
                stage_name,
                str(exc)[
                    :5000
                ],
            )


            raise


        finally:

            engine.dispose()


    # TASK 6 — BUILD GOLD

    @task
    def build_gold(
        pipeline_run_id
    ):

        record_pipeline_status(
            pipeline_run_id,
            None,
            "RUNNING",
            "build_gold",
        )


        run_pipeline_script(
            GOLD_SCRIPT,
            pipeline_run_id,
            "build_gold",
        )

    # TASK 7 — VALIDATE GOLD DAN RECORD PIPELINE METADATA

    @task
    def validate_gold_and_record_metadata(
        pipeline_run_id
    ):

        stage_name = (
            "validate_gold_and_record_metadata"
        )


        record_stage_status(
            pipeline_run_id,
            stage_name,
            "RUNNING",
        )


        engine = get_engine()


        try:

            with engine.connect() as connection:

                # GOLD TABLE COUNTS

                gold_counts = {}


                gold_tables = [
                    "order_360",
                    "customer_daily",
                    "product_daily",
                    "channel_campaign_daily",
                    "executive_kpis_daily",
                ]


                for table_name in gold_tables:

                    count = (
                        connection.execute(
                            text(
                                f"""
                                SELECT COUNT(*)
                                FROM gold.{table_name}
                                """
                            )
                        ).scalar()
                    )


                    gold_counts[
                        table_name
                    ] = int(count)


                # ORDER 360 MUST MATCH SILVER ORDERS

                silver_orders = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.orders
                        """)
                    ).scalar()
                )


                gold_orders = (
                    gold_counts[
                        "order_360"
                    ]
                )


                if (
                    gold_orders
                    != silver_orders
                ):

                    raise AirflowException(
                        "Gold validation gagal: "
                        "order_360 row count "
                        f"{gold_orders} != "
                        "silver.orders "
                        f"{silver_orders}"
                    )
                
                # DUPLICATE GOLD GRAIN

                duplicate_order_360 = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM
                            (
                                SELECT
                                    order_id
                                FROM gold.order_360
                                GROUP BY order_id
                                HAVING COUNT(*) > 1
                            ) q
                        """)
                    ).scalar()
                )


                if duplicate_order_360 != 0:

                    raise AirflowException(
                        "Gold validation gagal: "
                        "duplicate order_360 grain."
                    )


                # PIPELINE RUN ID CHECK

                wrong_pipeline_run = (
                    connection.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM gold.order_360
                            WHERE pipeline_run_id
                                  <> :pipeline_run_id
                               OR pipeline_run_id
                                  IS NULL
                        """),
                        {
                            "pipeline_run_id":
                                pipeline_run_id
                        }
                    ).scalar()
                )


                if wrong_pipeline_run != 0:

                    raise AirflowException(
                        "Gold validation gagal: "
                        f"{wrong_pipeline_run} rows "
                        "memiliki pipeline_run_id "
                        "yang salah."
                    )

                # RECORD PIPELINE METRICS
                connection.execute(
                    text("""
                        CREATE TABLE
                        IF NOT EXISTS
                        ops.pipeline_metrics
                        (
                            pipeline_run_id TEXT
                                NOT NULL,

                            metric_name TEXT
                                NOT NULL,

                            metric_value NUMERIC,

                            recorded_at_utc
                                TIMESTAMPTZ
                                NOT NULL
                                DEFAULT NOW(),

                            PRIMARY KEY
                            (
                                pipeline_run_id,
                                metric_name
                            )
                        )
                    """)
                )


                metrics = {

                    "silver_orders":
                        int(
                            silver_orders
                        ),

                    "gold_order_360":
                        int(
                            gold_counts[
                                "order_360"
                            ]
                        ),

                    "gold_customer_daily":
                        int(
                            gold_counts[
                                "customer_daily"
                            ]
                        ),

                    "gold_product_daily":
                        int(
                            gold_counts[
                                "product_daily"
                            ]
                        ),

                    "gold_channel_campaign_daily":
                        int(
                            gold_counts[
                                "channel_campaign_daily"
                            ]
                        ),

                    "gold_executive_kpis_daily":
                        int(
                            gold_counts[
                                "executive_kpis_daily"
                            ]
                        ),
                }


                for (
                    metric_name,
                    metric_value
                ) in metrics.items():

                    connection.execute(
                        text("""
                            INSERT INTO
                            ops.pipeline_metrics
                            (
                                pipeline_run_id,
                                metric_name,
                                metric_value,
                                recorded_at_utc
                            )

                            VALUES
                            (
                                :pipeline_run_id,
                                :metric_name,
                                :metric_value,
                                NOW()
                            )

                            ON CONFLICT
                            (
                                pipeline_run_id,
                                metric_name
                            )

                            DO UPDATE SET

                                metric_value =
                                    EXCLUDED.metric_value,

                                recorded_at_utc =
                                    NOW()
                        """),

                        {
                            "pipeline_run_id":
                                pipeline_run_id,

                            "metric_name":
                                metric_name,

                            "metric_value":
                                metric_value,
                        }
                    )
            
            # FINAL SUCCESS STATUS

            record_stage_status(
                pipeline_run_id,
                stage_name,
                "SUCCESS",
            )


            record_pipeline_status(
                pipeline_run_id=
                    pipeline_run_id,

                dag_run_id=
                    None,

                status=
                    "SUCCESS",

                stage=
                    "pipeline_completed",
            )


            print()
            print(
                "=" * 60
            )

            print(
                "PIPELINE QUALITY GATE: PASS"
            )

            print(
                "Pipeline Run ID:",
                pipeline_run_id
            )

            print(
                "Pipeline Status: SUCCESS"
            )

            print(
                "=" * 60
            )


        except Exception as exc:

            record_stage_status(
                pipeline_run_id,
                stage_name,
                "FAILED",
                str(exc)[
                    :5000
                ],
            )


            record_pipeline_status(
                pipeline_run_id=
                    pipeline_run_id,

                dag_run_id=
                    None,

                status=
                    "FAILED",

                stage=
                    stage_name,

                error_message=
                    str(exc)[
                        :5000
                    ],
            )


            raise


        finally:

            engine.dispose()

    # 10. TASK DEPENDENCIES

    pipeline_run = create_pipeline_run()


    validate_raw = (
        validate_raw_files(
            pipeline_run
        )
    )


    initialize_db = (
        initialize_postgresql_schemas(
            pipeline_run
        )
    )


    bronze_task = (
        load_bronze(
            pipeline_run
        )
    )


    silver_task = (
        build_silver(
            pipeline_run
        )
    )


    quality_task = (
        silver_quality_gate(
            pipeline_run
        )
    )


    gold_task = (
        build_gold(
            pipeline_run
        )
    )


    final_validation = (
        validate_gold_and_record_metadata(
            pipeline_run
        )
    )


    # DEPENDENCY WAJIB

    (
        validate_raw
        >> initialize_db
        >> bronze_task
        >> silver_task
        >> quality_task
        >> gold_task
        >> final_validation
    )