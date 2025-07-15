import os
import duckdb
from transform import transform_file
import logging
from datetime import datetime
import pandas as pd
from validate import validate_data_quality
from loader import save_transformed_data

# Set up logging
logging.basicConfig(
    filename='ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
RAW_DATA_DIR = 'data/raw'
CHECKPOINT_FILE = 'last_ingested.txt'
VALID_READING_TYPES = {"temperature", "humidity"}

# Load last checkpoint
def get_last_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return f.read().strip()
    return ""

def update_checkpoint(latest_file):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(latest_file)

# Validate schema
EXPECTED_COLUMNS = ['sensor_id', 'timestamp', 'reading_type', 'value', 'battery_level']

def is_valid_schema(columns):
    return all(col in columns for col in EXPECTED_COLUMNS)

# Ingestion summary
def log_summary(con, df):
    con.execute("CREATE OR REPLACE TEMP TABLE data AS SELECT * FROM df")
    summary = con.execute("""
        SELECT 
            COUNT(*) AS total_records,
            SUM(CASE WHEN value IS NULL OR battery_level IS NULL THEN 1 ELSE 0 END) AS nulls,
            COUNT(DISTINCT sensor_id) AS unique_sensors
        FROM data
    """).fetchdf()
    logging.info(f"Ingestion Summary:\n{summary}")

# Process individual file
def process_file(file_path):
    logging.info(f"Processing file: {file_path}")
    con = duckdb.connect(database=':memory:')
    
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").fetchdf()
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {e}")
        return 0, 0
    
    if not is_valid_schema(df.columns):
        logging.error(f"Schema mismatch in {file_path}")
        return 0, 0

    # Filter invalid rows
    initial_count = len(df)
    df = df[df['reading_type'].isin(VALID_READING_TYPES)]
    df = df.dropna(subset=['value', 'battery_level'])

    processed_count = len(df)
    skipped = initial_count - processed_count

    # Log via DuckDB
    log_summary(con, df)

    # Trigger transformation
    transformed_df = transform_file(file_path)

    # Save transformed data
    processed_path = file_path.replace("raw", "processed").replace(".parquet", "_transformed.parquet")
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    transformed_df.to_parquet(processed_path, index=False)
    logging.info(f"Transformed file saved: {processed_path}")
    validate_data_quality(processed_path)
    
    save_transformed_data(transformed_df)
    
    return len(transformed_df), 0

# Main ingestion function
def ingest_all():
    files = sorted(os.listdir(RAW_DATA_DIR))
    last_file = get_last_checkpoint()
    
    start = files.index(last_file) + 1 if last_file in files else 0
    new_files = files[start:]

    total_files = 0
    total_processed = 0
    total_skipped = 0

    for file in new_files:
        if not file.endswith('.parquet'):
            continue
        file_path = os.path.join(RAW_DATA_DIR, file)
        processed, skipped = process_file(file_path)
        if processed + skipped > 0:
            total_files += 1
            total_processed += processed
            total_skipped += skipped
            update_checkpoint(file)

    logging.info(f"Files read: {total_files}")
    logging.info(f"Records processed: {total_processed}")
    logging.info(f"Records skipped: {total_skipped}")

# Run ingestion
if __name__ == "__main__":
    ingest_all()
