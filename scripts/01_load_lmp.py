import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load API keys from .env file
load_dotenv(r"C:\文件\MISO_Trading_Analysis\.env")
MISO_PRICING_KEY = os.getenv("MISO_PRICING_KEY")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\01_load_lmp.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths / constants
DB_PATH     = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
TARGET_NODE = "MICHIGAN.HUB"
BASE_URL    = "https://apim.misoenergy.org/pricing/v1"
HEADERS     = {
    "Cache-Control": "no-cache",
    "Ocp-Apim-Subscription-Key": MISO_PRICING_KEY
}


def fetch_lmp(date_str, market):
    """
    Fetch hourly LMP for MICHIGAN.HUB from MISO Pricing API.
    market: 'day-ahead' or 'real-time'
    Returns a list of dicts with timestamp, lmp, mcc, mec, mlc.
    """
    url = f"{BASE_URL}/{market}/{date_str}/lmp-expost"
    params = {"node": TARGET_NODE, "timeResolution": "hourly", "pageNumber": 1}
    results = []

    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"  API error for {market} {date_str}: {e}")
            return None

        for row in data.get("data", []):
            ts = pd.to_datetime(row["timeInterval"]["start"])
            results.append({
                "timestamp":            ts.strftime("%Y-%m-%d %H:%M:%S"),
                "lmp_price":            row.get("lmp"),
                "energy_component":     row.get("mec"),
                "congestion_component": row.get("mcc"),
                "loss_component":       row.get("mlc"),
            })

        page = data.get("page", {})
        if page.get("lastPage", True):
            break
        params["pageNumber"] = params["pageNumber"] + 1

    return results


def validate_lmp_data(df, label):
    errors = []
    if df["lmp_price"].isnull().any():
        errors.append("NULL prices found")
    if (df["lmp_price"] < -500).any():
        errors.append(f"{(df['lmp_price'] < -500).sum()} prices below -500 $/MWh")
    if (df["lmp_price"] > 5000).any():
        errors.append(f"{(df['lmp_price'] > 5000).sum()} prices above 5000 $/MWh")
    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING [{label}]: {e}")
    else:
        log.info(f"  Validation passed for {label}")


def load_lmp_to_db(conn, rows, market_id, loadzone_id):
    cursor = conn.cursor()
    inserted = skipped = 0
    for row in rows:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hourly_lmp
                    (timestamp, loadzone_id, market_id, lmp_price,
                     energy_component, congestion_component, loss_component)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row["timestamp"], loadzone_id, market_id,
                row["lmp_price"], row["energy_component"],
                row["congestion_component"], row["loss_component"]
            ))
            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            log.error(f"  Row insert error: {e}")
    conn.commit()
    return inserted, skipped


def process_date_range(conn, start_date, end_date, market, market_id, market_label, loadzone_id):
    """Loop through date range and fetch LMP for each day."""
    current = start_date
    total_inserted = total_skipped = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        log.info(f"  Fetching {market_label} LMP for {date_str}...")

        rows = fetch_lmp(date_str, market)

        if not rows:
            log.warning(f"  No data returned for {date_str}")
            current += timedelta(days=1)
            continue

        df = pd.DataFrame(rows)
        validate_lmp_data(df, f"{market_label} {date_str}")

        ins, skp = load_lmp_to_db(conn, rows, market_id, loadzone_id)
        total_inserted += ins
        total_skipped  += skp
        log.info(f"  Inserted: {ins} | Skipped: {skp}")

        current += timedelta(days=1)

    log.info(f"{market_label} TOTAL — Inserted: {total_inserted} | Skipped: {total_skipped}")


def main():
    os.makedirs(r"C:\文件\MISO_Trading_Analysis\logs", exist_ok=True)

    log.info("=" * 60)
    log.info("01_load_lmp.py — MISO DA/RT LMP Loader (API)")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Target node: {TARGET_NODE}")
    log.info("=" * 60)

    if not MISO_PRICING_KEY:
        log.error("MISO_PRICING_KEY not found in .env file")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get loadzone_id
    cursor.execute("SELECT loadzone_id FROM loadzones WHERE loadzone_code = 'LRZ2_7'")
    result = cursor.fetchone()
    if not result:
        log.error("LRZ2_7 not found in database")
        conn.close()
        return
    loadzone_id = result[0]

    # Get last loaded date to avoid re-fetching everything
    cursor.execute("SELECT MAX(timestamp) FROM hourly_lmp WHERE market_id = 1")
    last_da = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(timestamp) FROM hourly_lmp WHERE market_id = 2")
    last_rt = cursor.fetchone()[0]

    # Default: fetch 2023-01-01 to yesterday if no data exists
    # Otherwise fetch from day after last loaded date
    yesterday = datetime.now() - timedelta(days=1)

    da_start = (pd.to_datetime(last_da) + timedelta(days=1)).date() if last_da else datetime(2023, 1, 1).date()
    rt_start = (pd.to_datetime(last_rt) + timedelta(days=1)).date() if last_rt else datetime(2023, 1, 1).date()
    end_date = yesterday.date()

    log.info(f"DA fetch range: {da_start} to {end_date}")
    log.info(f"RT fetch range: {rt_start} to {end_date}")

    # Fetch DA LMP (market_id = 1)
    process_date_range(conn, da_start, end_date, "day-ahead", 1, "DA", loadzone_id)

    # Fetch RT LMP (market_id = 2) — RT final available ~5 days lag
    rt_end = (datetime.now() - timedelta(days=5)).date()
    process_date_range(conn, rt_start, rt_end, "real-time", 2, "RT", loadzone_id)

    # Summary
    cursor.execute("SELECT market_id, COUNT(*) FROM hourly_lmp GROUP BY market_id")
    for market_id, count in cursor.fetchall():
        label = "DA" if market_id == 1 else "RT"
        log.info(f"  {label}: {count:,} rows")

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM hourly_lmp WHERE market_id = 1")
    log.info(f"  DA range: {cursor.fetchone()}")
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM hourly_lmp WHERE market_id = 2")
    log.info(f"  RT range: {cursor.fetchone()}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()