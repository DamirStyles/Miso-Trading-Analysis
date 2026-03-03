import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load API keys
load_dotenv(r"C:\文件\MISO_Trading_Analysis\.env")
MISO_LGI_KEY = os.getenv("MISO_LGI_KEY")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\05_load_load_data.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths / constants
DB_PATH      = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
BASE_URL     = "https://apim.misoenergy.org/lgi/v1"
HEADERS      = {
    "Cache-Control": "no-cache",
    "Ocp-Apim-Subscription-Key": MISO_LGI_KEY
}
TARGET_ZONE  = "LRZ2_7"


def fetch_actual_load(date_str):
    """Fetch hourly actual load for NORTH region (Michigan) for a given date."""
    url = f"{BASE_URL}/real-time/{date_str}/demand/actual"
    params = {"region": "NORTH", "timeResolution": "hourly", "pageNumber": 1}
    results = []

    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"  API error (actual load {date_str}): {e}")
            return None

        for row in data.get("data", []):
            ts = pd.to_datetime(row["timeInterval"]["start"])
            results.append({
                "timestamp":        ts.strftime("%Y-%m-%d %H:%M:%S"),
                "actual_load_mw":   row.get("load")
            })

        if data.get("page", {}).get("lastPage", True):
            break
        params["pageNumber"] += 1

    return results


def fetch_load_forecast(date_str):
    """Fetch hourly load forecast for NORTH region (Michigan) for a given date."""
    url = f"{BASE_URL}/forecast/{date_str}/load"
    params = {"region": "NORTH", "timeResolution": "hourly", "pageNumber": 1}
    results = []

    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"  API error (load forecast {date_str}): {e}")
            return None

        for row in data.get("data", []):
            ts = pd.to_datetime(row["timeInterval"]["start"])
            results.append({
                "timestamp":            ts.strftime("%Y-%m-%d %H:%M:%S"),
                "forecasted_load_mw":   row.get("loadForecast")
            })

        if data.get("page", {}).get("lastPage", True):
            break
        params["pageNumber"] += 1

    return results


def validate_load_data(df, date_str):
    errors = []
    null_pct_mtlf = df["forecasted_load_mw"].isnull().mean() * 100
    null_pct_act  = df["actual_load_mw"].isnull().mean() * 100
    if null_pct_mtlf > 5:
        errors.append(f"High NULL rate in forecast: {null_pct_mtlf:.1f}%")
    if null_pct_act > 5:
        errors.append(f"High NULL rate in actual: {null_pct_act:.1f}%")
    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING [{date_str}]: {e}")
    else:
        log.info(f"  Validation passed for {date_str}")


def load_data_to_db(conn, df, loadzone_id):
    cursor = conn.cursor()
    inserted = skipped = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_load
                    (timestamp, loadzone_id, forecasted_load_mw, actual_load_mw)
                VALUES (?, ?, ?, ?)
            """, (
                row["timestamp"], loadzone_id,
                float(row["forecasted_load_mw"]) if pd.notna(row["forecasted_load_mw"]) else None,
                float(row["actual_load_mw"])      if pd.notna(row["actual_load_mw"])      else None,
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
    log.info("05_load_load_data.py — MISO Load Loader (API)")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Target zone: {TARGET_ZONE} (Michigan/North)")
    log.info("=" * 60)

    if not MISO_LGI_KEY:
        log.error("MISO_LGI_KEY not found in .env file")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get loadzone_id
    cursor.execute("SELECT loadzone_id FROM loadzones WHERE loadzone_code = ?", (TARGET_ZONE,))
    result = cursor.fetchone()
    if not result:
        log.error(f"Loadzone {TARGET_ZONE} not found in database.")
        conn.close()
        return
    loadzone_id = result[0]
    log.info(f"Loadzone ID: {loadzone_id}")

    # Get last loaded date
    cursor.execute("SELECT MAX(timestamp) FROM hourly_load WHERE loadzone_id = ?", (loadzone_id,))
    last_loaded = cursor.fetchone()[0]

    yesterday  = datetime.now() - timedelta(days=1)
    start_date = (pd.to_datetime(last_loaded) + timedelta(days=1)).date() if last_loaded else datetime(2023, 1, 1).date()
    end_date   = yesterday.date()

    if start_date > end_date:
        log.info("Load data already up to date.")
        conn.close()
        return

    log.info(f"Fetching load data from {start_date} to {end_date}...")

    total_inserted = total_skipped = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        log.info(f"  Processing {date_str}...")

        actual   = fetch_actual_load(date_str)
        forecast = fetch_load_forecast(date_str)

        if not actual or not forecast:
            log.warning(f"  Skipping {date_str} — missing data")
            current += timedelta(days=1)
            continue

        # Merge actual and forecast on timestamp
        df_actual   = pd.DataFrame(actual)
        df_forecast = pd.DataFrame(forecast)
        df = pd.merge(df_actual, df_forecast, on="timestamp", how="outer")

        validate_load_data(df, date_str)

        ins, skp = load_data_to_db(conn, df, loadzone_id)
        total_inserted += ins
        total_skipped  += skp
        log.info(f"  Inserted: {ins} | Skipped: {skp}")

        current += timedelta(days=1)

    log.info(f"TOTAL — Inserted: {total_inserted:,} | Skipped: {total_skipped:,}")

    cursor.execute("SELECT COUNT(*) FROM hourly_load")
    log.info(f"Total rows in DB: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM hourly_load")
    log.info(f"Date range: {cursor.fetchone()}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()