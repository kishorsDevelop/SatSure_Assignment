import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta

# --- CONFIG ---

# Expected range per reading_type
EXPECTED_RANGE = {
    'temperature': (10, 50),    # in Celsius
    'humidity': (20, 90)        # in percent
}

# Calibration params (hardcoded example)
CALIBRATION_PARAMS = {
    'temperature': {'multiplier': 1.02, 'offset': -0.5},
    'humidity': {'multiplier': 0.97, 'offset': 1.0}
}

# Timezone adjustment
TIMEZONE_OFFSET = timedelta(hours=5, minutes=30)

# --- FUNCTIONS ---

def load_data_from_parquet(file_path: str) -> pd.DataFrame:
    return duckdb.query(f"SELECT * FROM read_parquet('{file_path}')").to_df()

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    # Drop rows with critical missing values
    df = df.dropna(subset=['sensor_id', 'timestamp', 'reading_type', 'value'])

    # Outlier detection (Z-score)
    df['z_score'] = df.groupby('reading_type')['value'].transform(lambda x: (x - x.mean()) / x.std())
    df = df[df['z_score'].abs() <= 3].drop(columns='z_score')

    return df

def normalize_value(row):
    params = CALIBRATION_PARAMS.get(row['reading_type'], {'multiplier': 1.0, 'offset': 0.0})
    return row['value'] * params['multiplier'] + params['offset']

def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    # 🔑 Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Drop rows where timestamp couldn't be parsed
    df = df.dropna(subset=['timestamp'])

    # Normalize
    df['normalized_value'] = df.apply(normalize_value, axis=1)

    # Daily average per sensor + type
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')

    daily_avg = df.groupby(['sensor_id', 'reading_type', 'date'])['normalized_value'].mean().reset_index()
    daily_avg.rename(columns={'normalized_value': 'daily_avg'}, inplace=True)
    df = pd.merge(df, daily_avg, on=['sensor_id', 'reading_type', 'date'], how='left')

    # Sort for rolling window
    df = df.sort_values(['sensor_id', 'timestamp'])

    # 7-day rolling average (using time-based window)
    df['rolling_7d_avg'] = df.groupby('sensor_id')['normalized_value'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

    # Anomalous readings
    def is_anomalous(row):
        low, high = EXPECTED_RANGE.get(row['reading_type'], (float('-inf'), float('inf')))
        return not (low <= row['value'] <= high)

    df['anomalous_reading'] = df.apply(is_anomalous, axis=1)

    return df

def process_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    # Convert to datetime if not already
    if df['timestamp'].dtype == object:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    # Convert to UTC+5:30
    df['timestamp'] = df['timestamp'] + TIMEZONE_OFFSET
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')  # ISO 8601

    return df

def transform_file(file_path: str) -> pd.DataFrame:
    df = load_data_from_parquet(file_path)
    df = clean_data(df)
    df = process_timestamps(df)
    df = add_derived_fields(df)
    return df
