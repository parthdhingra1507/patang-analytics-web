"""
Convert NDJSON exports to Parquet using DuckDB.
"""

import glob
import os
import duckdb

ndjson_dir = os.environ.get("NDJSON_DIR", "analytics_work/ndjson")
parquet_dir = os.environ.get("PARQUET_DIR", "analytics_store/parquet")
run_id = os.environ.get("RUN_ID")

if not run_id:
    raise SystemExit("RUN_ID is required")

os.makedirs(parquet_dir, exist_ok=True)

files = sorted(glob.glob(os.path.join(ndjson_dir, "*.ndjson")))
for file_path in files:
    table = os.path.splitext(os.path.basename(file_path))[0]
    table_dir = os.path.join(parquet_dir, table)
    os.makedirs(table_dir, exist_ok=True)
    out_file = os.path.join(table_dir, f"{run_id}.parquet")
    duckdb.sql(
        f"COPY (SELECT * FROM read_json_auto('{file_path}')) TO '{out_file}' (FORMAT PARQUET)"
    )
