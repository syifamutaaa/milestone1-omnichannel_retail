CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    raw_record_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    pipeline_run_id TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,

    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,

    source_record_id TEXT,

    record_checksum TEXT NOT NULL,

    raw_payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_lineage
ON bronze.raw_records (
    pipeline_run_id,
    source_file,
    source_row_number
);
--CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_pipeline_run
    --ON bronze.raw_records (pipeline_run_id);

--CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_source_file
    --ON bronze.raw_records (source_file);

--CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_source_record_id
    --ON bronze.raw_records (source_record_id);

--CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_checksum
    --ON bronze.raw_records (record_checksum);