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
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\06_load_outages.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths / constants
DB_PATH  = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
BASE_URL = "https://apim.misoenergy.org/lgi/v1"
HEADERS  = {
    "Cache-Control": "no-cache",
    "Ocp-Apim-Subscription-Key": MISO_LGI_KEY
}


def fetch_outages(date_str):
    """
    Fetch hourly real-time outages from MISO API for a given date.
    Returns list of dicts with timestamp, forced_outages_mw, planned_outages_mw.
    """
    url = f"{BASE_URL}/real-time/{date_str}/outage"
    params = {"pageNumber": 1}
    results = []

    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"  API error (outages {date_str}): {e}")
            return None

        for row in data.get("data", []):
            ts = pd.to_datetime(row["timeInterval"]["start"])
            results.append({
                "timestamp":            ts.strftime("%Y-%m-%d %H:%M:%S"),
                "region":               "MISO",
                "forced_outages_mw":    row.get("realTime"),
                "planned_outages_mw":   row.get("forward"),
                "unplanned_outages_mw": None,
                "derated_outages_mw":   None,
            })

        if data.get("page", {}).get("lastPage", True):
            break
        params["pageNumber"] += 1

    return results


def validate_outage_data(df, date_str):
    errors = []
    for col in ["forced_outages_mw", "planned_outages_mw"]:
        null_pct = df[col].isnull().mean() * 100
        if null_pct > 10:
            errors.append(f"High NULL rate in {col}: {null_pct:.1f}%")
    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING [{date_str}]: {e}")
    else:
        log.info(f"  Validation passed for {date_str}")


def load_outages_to_db(conn, rows):
    cursor = conn.cursor()
    inserted = skipped = 0
    for row in rows:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_outages
                    (timestamp, region, forced_outages_mw, planned_outages_mw,
                     unplanned_outages_mw, derated_outages_mw)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row["timestamp"],
                row["region"],
                float(row["forced_outages_mw"])    if row["forced_outages_mw"]    is not None else None,
                float(row["planned_outages_mw"])   if row["planned_outages_mw"]   is not None else None,
                None,
                None,
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
    log.info("06_load_outages.py — MISO Outages Loader (API)")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    if not MISO_LGI_KEY:
        log.error("MISO_LGI_KEY not found in .env file")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get last loaded date
    cursor.execute("SELECT MAX(timestamp) FROM hourly_outages")
    last_loaded = cursor.fetchone()[0]

    yesterday  = datetime.now() - timedelta(days=1)
    start_date = (pd.to_datetime(last_loaded) + timedelta(days=1)).date() if last_loaded else datetime(2023, 1, 1).date()
    end_date   = yesterday.date()

    if start_date > end_date:
        log.info("Outage data already up to date.")
        conn.close()
        return

    log.info(f"Fetching outages from {start_date} to {end_date}...")

    total_inserted = total_skipped = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        log.info(f"  Processing {date_str}...")

        rows = fetch_outages(date_str)

        if not rows:
            log.warning(f"  No data for {date_str}")
            current += timedelta(days=1)
            continue

        df = pd.DataFrame(rows)
        validate_outage_data(df, date_str)

        ins, skp = load_outages_to_db(conn, rows)
        total_inserted += ins
        total_skipped  += skp
        log.info(f"  Inserted: {ins} | Skipped: {skp}")

        current += timedelta(days=1)

    log.info(f"TOTAL — Inserted: {total_inserted:,} | Skipped: {total_skipped:,}")

    cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM hourly_outages")
    total, dmin, dmax = cursor.fetchone()
    log.info(f"Total rows in DB: {total:,} | Range: {dmin} to {dmax}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()