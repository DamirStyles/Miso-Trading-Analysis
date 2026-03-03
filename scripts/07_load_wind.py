import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\07_load_wind.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH          = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
WIND_ACT_FOLDER  = r"C:\文件\MISO_Trading_Analysis\data\raw\Wind Data Actual 2023-2025"
WIND_FCST_FILE   = r"C:\文件\MISO_Trading_Analysis\data\raw\DA Wind Forecasts 2023-2025\MISO_wind_forecast_2023_2025.csv"


def load_wind_actual(conn):
    """Load hwd_HIST wind actual files into hourly_wind_actual table."""
    cursor = conn.cursor()

    files = sorted([f for f in os.listdir(WIND_ACT_FOLDER) if f.endswith(".csv")])
    log.info(f"Found {len(files)} wind actual files")

    total_inserted = 0
    total_skipped  = 0

    for filename in files:
        filepath = os.path.join(WIND_ACT_FOLDER, filename)
        log.info(f"Loading {filename}...")

        try:
            df = pd.read_csv(filepath, skiprows=7, low_memory=False, on_bad_lines='skip')
        except Exception as e:
            log.error(f"  Failed to read {filename}: {e}")
            continue

        # Strip tab from column name
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={"Market Day": "market_day", "Hour Ending": "hour", "MWh": "wind_actual_mwh"})

        # Drop any non-data rows
        df = df.dropna(subset=["market_day", "hour"])
        df = df[df["market_day"] != "Market Day"].copy()

        # Parse timestamp
        df["market_day"] = pd.to_datetime(df["market_day"], errors="coerce")
        df["hour"]       = pd.to_numeric(df["hour"], errors="coerce")
        df = df.dropna(subset=["market_day", "hour"])

        df["timestamp"] = df["market_day"] + pd.to_timedelta(df["hour"], unit="h")
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df["wind_actual_mwh"] = pd.to_numeric(df["wind_actual_mwh"], errors="coerce")

        rows_inserted = 0
        rows_skipped  = 0

        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO hourly_wind_actual
                        (timestamp, wind_actual_mwh)
                    VALUES (?, ?)
                """, (
                    row["timestamp"],
                    float(row["wind_actual_mwh"]) if pd.notna(row["wind_actual_mwh"]) else None,
                ))
                if cursor.rowcount == 1:
                    rows_inserted += 1
                else:
                    rows_skipped += 1
            except Exception as e:
                log.error(f"  Row insert error: {e}")

        conn.commit()
        total_inserted += rows_inserted
        total_skipped  += rows_skipped
        log.info(f"  Inserted: {rows_inserted:,} | Skipped: {rows_skipped:,}")

    log.info(f"Wind actual TOTAL — Inserted: {total_inserted:,} | Skipped: {total_skipped:,}")

    cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM hourly_wind_actual")
    total, date_min, date_max = cursor.fetchone()
    log.info(f"Total rows in hourly_wind_actual: {total:,} | Range: {date_min} to {date_max}")


def load_wind_forecast(conn):
    """Load MISO wind forecast CSV into hourly_wind_forecast table."""
    cursor = conn.cursor()

    log.info("Reading wind forecast file...")
    df = pd.read_csv(WIND_FCST_FILE, low_memory=False)
    log.info(f"Raw rows loaded: {len(df):,}")

    # Parse timestamp from Interval Start, strip timezone
    df["timestamp"] = pd.to_datetime(df["Interval Start"], utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert MW columns to numeric
    for col in ["North", "Central", "South", "MISO"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter to 2023-2025
    df["ts_naive"] = pd.to_datetime(df["timestamp"])
    df = df[(df["ts_naive"] >= "2023-01-01") & (df["ts_naive"] <= "2025-12-31")].copy()
    log.info(f"Rows after filtering to 2023-2025: {len(df):,}")

    rows_inserted = 0
    rows_skipped  = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_wind_forecast
                    (timestamp, north_mw, central_mw, south_mw, miso_total_mw)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row["timestamp"],
                float(row["North"])   if pd.notna(row["North"])   else None,
                float(row["Central"]) if pd.notna(row["Central"]) else None,
                float(row["South"])   if pd.notna(row["South"])   else None,
                float(row["MISO"])    if pd.notna(row["MISO"])    else None,
            ))
            if cursor.rowcount == 1:
                rows_inserted += 1
            else:
                rows_skipped += 1
        except Exception as e:
            log.error(f"  Row insert error: {e}")

    conn.commit()
    log.info(f"Wind forecast — Inserted: {rows_inserted:,} | Skipped: {rows_skipped:,}")

    cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM hourly_wind_forecast")
    total, date_min, date_max = cursor.fetchone()
    log.info(f"Total rows in hourly_wind_forecast: {total:,} | Range: {date_min} to {date_max}")

    cursor.execute("SELECT timestamp, north_mw, central_mw, south_mw, miso_total_mw FROM hourly_wind_forecast LIMIT 3")
    log.info("Sample forecast rows:")
    for row in cursor.fetchall():
        log.info(f"  {row}")


def main():
    log.info("=" * 60)
    log.info("07_load_wind.py — MISO Wind Actual + Forecast Loader")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    load_wind_actual(conn)
    load_wind_forecast(conn)

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()