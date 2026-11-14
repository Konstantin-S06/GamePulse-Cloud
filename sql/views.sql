CREATE OR REPLACE VIEW v_bronze_events AS
SELECT * FROM read_parquet('{ROOT}/bronze/events/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_silver_events AS
SELECT * FROM read_parquet('{ROOT}/silver/events/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_quarantine_events AS
SELECT * FROM read_parquet('{ROOT}/quarantine/events/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_quarantine_ingestion AS
SELECT * FROM read_parquet('{ROOT}/quarantine/ingestion/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_dim_character AS
SELECT * FROM read_parquet('{ROOT}/gold/dim_character/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_dim_item AS
SELECT * FROM read_parquet('{ROOT}/gold/dim_item/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_dim_date AS
SELECT * FROM read_parquet('{ROOT}/gold/dim_date/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_dim_player AS
SELECT * FROM read_parquet('{ROOT}/gold/dim_player/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_dim_match AS
SELECT * FROM read_parquet('{ROOT}/gold/dim_match/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_fact_match_events AS
SELECT * FROM read_parquet('{ROOT}/gold/fact_match_events/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_fact_player_match AS
SELECT * FROM read_parquet('{ROOT}/gold/fact_player_match/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_agg_daily_overview AS
SELECT * FROM read_parquet('{ROOT}/gold/agg_daily_overview/**/*.parquet', hive_partitioning=true, union_by_name=true);

-- split

CREATE OR REPLACE VIEW v_agg_character_daily AS
SELECT * FROM read_parquet('{ROOT}/gold/agg_character_daily/**/*.parquet', hive_partitioning=true, union_by_name=true);