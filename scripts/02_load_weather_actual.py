import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\02_load_weather_actual.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths / constants
DB_PATH      = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
STATION_CODE = "KDTW"
ASOS_URL     = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"


def fetch_weather(start_date, end_date):
    """
    Fetch sub-hourly weather from Iowa Mesonet ASOS API for DTW.
    Returns a DataFrame resampled to hourly.
    """
    params = {
        "station":    "DTW",
        "data":       "tmpf,dwpf,sknt",
        "year1":      start_date.year,
        "month1":     start_date.month,
        "day1":       start_date.day,
        "year2":      end_date.year,
        "month2":     end_date.month,
        "day2":       end_date.day,
        "tz":         "UTC",
        "format":     "comma",
        "latlon":     "no",
        "missing":    "M",
        "trace":      "T",
        "direct":     "no",
        "report_type": "1"
    }

    try:
        r = requests.get(ASOS_URL, params=params, timeout=60)
        r.raise_for_status()
    except Exception as e:
        log.error(f"  API error: {e}")
        return None

    # Skip comment lines starting with #
    lines = [l for l in r.text.splitlines() if not l.startswith("#")]
    df = pd.read_csv(StringIO("\n".join(lines)), low_memory=False)

    # Rename columns
    df = df.rename(columns={
        "valid": "timestamp_raw",
        "tmpf":  "temperature_f",
        "dwpf":  "dew_point_f",
        "sknt":  "wind_speed_knts"
    })

    # Coerce to numeric
    df["temperature_f"]   = pd.to_numeric(df["temperature_f"],   errors="coerce")
    df["dew_point_f"]     = pd.to_numeric(df["dew_point_f"],     errors="coerce")
    df["wind_speed_knts"] = pd.to_numeric(df["wind_speed_knts"], errors="coerce")

    # Parse timestamp and resample to hourly
    df["timestamp_raw"] = pd.to_datetime(df["timestamp_raw"], utc=True)
    df = df.set_index("timestamp_raw")
    df_hourly = df[["temperature_f", "dew_point_f", "wind_speed_knts"]].resample("1h").mean()
    df_hourly = df_hourly.reset_index().rename(columns={"timestamp_raw": "timestamp"})
    df_hourly["timestamp"] = df_hourly["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df_hourly


def validate_weather_data(df):
    errors = []
    if df["temperature_f"].isnull().all():
        errors.append("All temperature values are NULL")
    null_pct = df["temperature_f"].isnull().mean() * 100
    if null_pct > 10:
        errors.append(f"High NULL rate in temperature: {null_pct:.1f}%")
    valid = df["temperature_f"].dropna()
    if (valid < -30).any():
        errors.append(f"{(valid < -30).sum()} temperatures below -30F")
    if (valid > 115).any():
        errors.append(f"{(valid > 115).sum()} temperatures above 115F")
    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING: {e}")
    else:
        log.info("  Validation passed")


def load_weather_to_db(conn, df, station_id):
    cursor = conn.cursor()
    inserted = skipped = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_weather_actual
                    (timestamp, station_id, temperature_f, dew_point_f, wind_speed_knts)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row["timestamp"], station_id,
                float(row["temperature_f"])   if pd.notna(row["temperature_f"])   else None,
                float(row["dew_point_f"])     if pd.notna(row["dew_point_f"])     else None,
                float(row["wind_speed_knts"]) if pd.notna(row["wind_speed_knts"]) else None,
            ))
            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            log.error(f"  Row insert error: {e}")
    conn.commit()
    return inserted, skipped


def main():
    log.info("=" * 60)
    log.info("02_load_weather_actual.py — DTW Actual Weather Loader (API)")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get station_id
    cursor.execute("SELECT station_id FROM stations WHERE station_code = ?", (STATION_CODE,))
    result = cursor.fetchone()
    if not result:
        log.error(f"Station {STATION_CODE} not found in database.")
        conn.close()
        return
    station_id = result[0]
    log.info(f"Station ID for {STATION_CODE}: {station_id}")

    # Get last loaded date
    cursor.execute("SELECT MAX(timestamp) FROM hourly_weather_actual WHERE station_id = ?", (station_id,))
    last_loaded = cursor.fetchone()[0]

    yesterday = datetime.now() - timedelta(days=1)
    start_date = (pd.to_datetime(last_loaded) + timedelta(days=1)).date() if last_loaded else datetime(2023, 1, 1).date()
    end_date   = yesterday.date()

    if start_date > end_date:
        log.info("Weather data already up to date.")
        conn.close()
        return

    log.info(f"Fetching weather from {start_date} to {end_date}...")

    df = fetch_weather(start_date, end_date)

    if df is None or df.empty:
        log.error("No data returned from API.")
        conn.close()
        return

    log.info(f"Hourly rows fetched: {len(df):,}")
    validate_weather_data(df)

    inserted, skipped = load_weather_to_db(conn, df, station_id)
    log.info(f"Inserted: {inserted:,} | Skipped: {skipped:,}")

    cursor.execute("SELECT COUNT(*) FROM hourly_weather_actual")
    log.info(f"Total rows in DB: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM hourly_weather_actual")
    log.info(f"Date range: {cursor.fetchone()}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()