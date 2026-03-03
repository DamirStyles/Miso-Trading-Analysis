import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\03_load_weather_forecast.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths
DB_PATH       = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
FORECAST_FILE = r"C:\文件\MISO_Trading_Analysis\data\raw\DA Weather Temp Forecasts 2023-2025\KDTW_GFS_MOS_forecast_2023_2025.csv"
STATION_CODE  = "KDTW"


def validate_forecast_data(df):
    """Run data quality checks on forecast data."""
    errors = []

    null_pct = df["temp_forecast_f"].isnull().mean() * 100
    if null_pct > 20:
        errors.append(f"High NULL rate in forecast temperature: {null_pct:.1f}%")

    valid = df["temp_forecast_f"].dropna()
    if len(valid) > 0:
        if (valid < -30).any():
            errors.append(f"{(valid < -30).sum()} forecast temps below -30F")
        if (valid > 115).any():
            errors.append(f"{(valid > 115).sum()} forecast temps above 115F")

    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING: {e}")
    else:
        log.info("  Validation passed")

    return len(errors) == 0


def load_forecast_to_db(conn, df, station_id):
    """Insert forecast rows into hourly_weather_forecast table."""
    cursor = conn.cursor()
    rows_inserted = 0
    rows_skipped  = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_weather_forecast
                    (runtime, ftime, station_id, temp_forecast_f, dew_point_f, wind_speed_knts)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row["runtime"],
                row["ftime"],
                station_id,
                float(row["temp_forecast_f"])  if pd.notna(row["temp_forecast_f"])  else None,
                float(row["dew_point_f"])       if pd.notna(row["dew_point_f"])       else None,
                float(row["wind_speed_knts"])   if pd.notna(row["wind_speed_knts"])   else None,
            ))
            if cursor.rowcount == 1:
                rows_inserted += 1
            else:
                rows_skipped += 1
        except Exception as e:
            log.error(f"  Row insert error: {e} | row: {row.to_dict()}")

    conn.commit()
    return rows_inserted, rows_skipped


def main():
    log.info("=" * 60)
    log.info("03_load_weather_forecast.py — GFS MOS Forecast Loader")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get station_id for KDTW
    cursor.execute("SELECT station_id FROM stations WHERE station_code = ?", (STATION_CODE,))
    result = cursor.fetchone()
    if not result:
        log.error(f"Station {STATION_CODE} not found in database.")
        conn.close()
        return
    station_id = result[0]
    log.info(f"Station ID for {STATION_CODE}: {station_id}")

    # Load raw file
    log.info("Reading GFS MOS forecast file...")
    df = pd.read_csv(FORECAST_FILE, low_memory=False)
    log.info(f"Raw rows loaded: {len(df):,}")
    log.info(f"Columns: {df.columns.tolist()}")

    # Rename columns to match database schema
    df = df.rename(columns={
        "tmp": "temp_forecast_f",
        "dpt": "dew_point_f",
        "wsp": "wind_speed_knts"
    })

    # Convert to numeric — MOS data can have missing values
    df["temp_forecast_f"] = pd.to_numeric(df["temp_forecast_f"], errors="coerce")
    df["dew_point_f"]     = pd.to_numeric(df["dew_point_f"],     errors="coerce")
    df["wind_speed_knts"] = pd.to_numeric(df["wind_speed_knts"], errors="coerce")

    # Parse timestamps
    df["runtime"] = pd.to_datetime(df["runtime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["ftime"]   = pd.to_datetime(df["ftime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Filter to DA forecasts only — runtime issued at 00:00 UTC (midnight run)
    # This is the forecast the DA market uses for next day planning
    df["runtime_hour"] = pd.to_datetime(df["runtime"]).dt.hour
    df_da = df[df["runtime_hour"] == 0].copy()

    log.info(f"Total rows: {len(df):,}")
    log.info(f"DA forecast rows (runtime=00:00 UTC): {len(df_da):,}")
    log.info(f"Date range: {df_da['ftime'].min()} to {df_da['ftime'].max()}")

    # Validate
    validate_forecast_data(df_da)

    # Load into database
    log.info("Loading into hourly_weather_forecast table...")
    inserted, skipped = load_forecast_to_db(conn, df_da, station_id)
    log.info(f"Inserted: {inserted:,} rows | Skipped (duplicates): {skipped:,} rows")

    # Summary check
    cursor.execute("SELECT COUNT(*) FROM hourly_weather_forecast")
    total = cursor.fetchone()[0]
    log.info(f"Total rows in hourly_weather_forecast: {total:,}")

    cursor.execute("SELECT MIN(ftime), MAX(ftime) FROM hourly_weather_forecast")
    date_range = cursor.fetchone()
    log.info(f"Forecast valid time range: {date_range[0]} to {date_range[1]}")

    # Spot check
    cursor.execute("""
        SELECT runtime, ftime, temp_forecast_f, dew_point_f, wind_speed_knts
        FROM hourly_weather_forecast
        LIMIT 5
    """)
    log.info("Sample rows:")
    for row in cursor.fetchall():
        log.info(f"  {row}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()