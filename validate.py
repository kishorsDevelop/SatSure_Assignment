import duckdb
import pandas as pd
import os

EXPECTED_RANGE = {
    'temperature': (0, 50),
    'humidity': (0, 100),
    # Add more as needed
}

def validate_data_quality(parquet_path: str, output_csv: str = "data_quality_report.csv"):
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE TABLE readings AS 
        SELECT * FROM read_parquet('{parquet_path}')
    """)

    # Type validation
    con.execute("""
        SELECT 
            COUNT(*) AS total_records,
            SUM(typeof(value) NOT IN ('DOUBLE', 'FLOAT')) AS invalid_value_type,
            SUM(typeof(timestamp) NOT IN ('TIMESTAMP', 'VARCHAR')) AS invalid_timestamp_type
        FROM readings
    """)
    type_validation = con.fetchdf()

    # Range validation
    range_checks = []
    for reading_type, (min_val, max_val) in EXPECTED_RANGE.items():
        query = f"""
            SELECT 
                '{reading_type}' AS reading_type,
                COUNT(*) AS total,
                SUM(value < {min_val} OR value > {max_val}) AS out_of_range
            FROM readings
            WHERE reading_type = '{reading_type}'
        """
        range_checks.append(con.execute(query).fetchdf())
    range_validation = pd.concat(range_checks)

    # Hourly gap detection
    gap_query = """
WITH readings_casted AS (
    SELECT 
        sensor_id, 
        reading_type, 
        CAST(timestamp AS TIMESTAMP) AS timestamp 
    FROM readings
),
expected_times AS (
    SELECT 
        sensor_id, 
        reading_type,
        UNNEST(generate_series(
            MIN(timestamp),
            MAX(timestamp),
            INTERVAL 1 HOUR
        )) AS expected_time
    FROM readings_casted
    GROUP BY sensor_id, reading_type
)
SELECT 
    e.sensor_id, 
    e.reading_type,
    COUNT(e.expected_time) AS expected_count,
    COUNT(r.timestamp) AS actual_count,
    COUNT(e.expected_time) - COUNT(r.timestamp) AS missing_hours
FROM expected_times e
LEFT JOIN readings_casted r 
    ON e.sensor_id = r.sensor_id 
    AND e.reading_type = r.reading_type 
    AND DATE_TRUNC('hour', r.timestamp) = e.expected_time
GROUP BY e.sensor_id, e.reading_type
"""




    hourly_gaps = con.execute(gap_query).fetchdf()

    # % missing values per reading_type
    missing_query = """
        SELECT reading_type,
               COUNT(*) AS total,
               SUM(value IS NULL OR value != value) AS missing_value_count,
               ROUND(100.0 * SUM(value IS NULL OR value != value) / COUNT(*), 2) AS missing_percentage
        FROM readings
        GROUP BY reading_type
    """
    missing_profile = con.execute(missing_query).fetchdf()

    # % anomalous readings (outside expected range)
    anomaly_query = " UNION ALL ".join([
        f"""
        SELECT
            '{rt}' AS reading_type,
            COUNT(*) AS total,
            SUM(value < {rng[0]} OR value > {rng[1]}) AS anomalies,
            ROUND(100.0 * SUM(value < {rng[0]} OR value > {rng[1]}) / COUNT(*), 2) AS anomaly_percent
        FROM readings
        WHERE reading_type = '{rt}'
        """
        for rt, rng in EXPECTED_RANGE.items()
    ])
    anomaly_stats = con.execute(anomaly_query).fetchdf()

    # Merge and save all stats
    with pd.ExcelWriter(output_csv.replace('.csv', '.xlsx')) as writer:
        type_validation.to_excel(writer, sheet_name="TypeValidation", index=False)
        range_validation.to_excel(writer, sheet_name="RangeValidation", index=False)
        hourly_gaps.to_excel(writer, sheet_name="HourlyGaps", index=False)
        missing_profile.to_excel(writer, sheet_name="MissingValues", index=False)
        anomaly_stats.to_excel(writer, sheet_name="AnomalyStats", index=False)

    # Also optionally save a CSV summary (choose one sheet)
    hourly_gaps.to_csv(output_csv, index=False)
    print(f"[✓] Data quality report written to: {output_csv}")
