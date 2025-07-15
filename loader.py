# loader.py
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def save_transformed_data(df: pd.DataFrame, output_root: str = "data/processed") -> None:
    """
    Saves transformed DataFrame to Parquet with partitioning and compression.

    Args:
        df (pd.DataFrame): Transformed DataFrame (must include 'timestamp' and 'sensor_id')
        output_root (str): Root path to store processed data
    """
    if "timestamp" not in df.columns or "sensor_id" not in df.columns:
        raise ValueError("DataFrame must include 'timestamp' and 'sensor_id' columns")

    # Extract date from timestamp for partitioning
    df["date"] = pd.to_datetime(df["timestamp"]).dt.strftime('%Y-%m-%d')

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=output_root,
        partition_cols=["date", "sensor_id"],
        compression="snappy"
    )

    print(f"Data saved to {output_root} partitioned by date and sensor_id")
