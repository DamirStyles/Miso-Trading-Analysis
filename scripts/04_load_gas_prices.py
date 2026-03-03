import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load API keys
load_dotenv(r"C:\文件\MISO_Trading_Analysis\.env")
EIA_KEY = os.getenv("EIA_KEY")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\04_load_gas_prices.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Paths / constants
DB_PATH  = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
EIA_URL  = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"


def fetch_gas_prices(start_date, end_date):
    """
    Fetch daily Henry Hub gas prices from EIA API.
    Returns a DataFrame with date and henry_hub_price columns.
    """
    params = {
        "api_key":              EIA_KEY,
        "frequency":            "daily",
        "data[0]":              "value",
        "facets[series][]":     "RNGWHHD",
        "start":                start_date.strftime("%Y-%m-%d"),
        "end":                  end_date.strftime("%Y-%m-%d"),
        "sort[0][column]":      "period",
        "sort[0][direction]":   "asc",
        "length":               5000
    }

    try:
        r = requests.get(EIA_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.error(f"  EIA API error: {e}")
        return None

    rows = data.get("response", {}).get("data", [])
    if not rows:
        log.warning("  No data returned from EIA API")
        return None

    df = pd.DataFrame(rows)[["period", "value"]]
    df.columns = ["date", "henry_hub_price"]
    df["henry_hub_price"] = pd.to_numeric(df["henry_hub_price"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    return df


def validate_gas_data(df):
    errors = []
    null_pct = df["henry_hub_price"].isnull().mean() * 100
    if null_pct > 10:
        errors.append(f"High NULL rate: {null_pct:.1f}%")
    valid = df["henry_hub_price"].dropna()
    if (valid <= 0).any():
        errors.append(f"{(valid <= 0).sum()} prices at or below zero")
    if (valid > 30).any():
        errors.append(f"{(valid > 30).sum()} prices above $30/MMBtu (extreme)")
    if errors:
        for e in errors:
            log.warning(f"  VALIDATION WARNING: {e}")
    else:
        log.info("  Validation passed")


def load_gas_to_db(conn, df):
    cursor = conn.cursor()
    inserted = skipped = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO daily_gas_prices
                    (date, henry_hub_price)
                VALUES (?, ?)
            """, (
                row["date"],
                float(row["henry_hub_price"]) if pd.notna(row["henry_hub_price"]) else None,
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
    log.info("04_load_gas_prices.py — Henry Hub Gas Price Loader (API)")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    if not EIA_KEY:
        log.error("EIA_KEY not found in .env file")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get last loaded date
    cursor.execute("SELECT MAX(date) FROM daily_gas_prices")
    last_loaded = cursor.fetchone()[0]

    yesterday  = datetime.now() - timedelta(days=1)
    start_date = (pd.to_datetime(last_loaded) + timedelta(days=1)).date() if last_loaded else datetime(2023, 1, 1).date()
    end_date   = yesterday.date()

    if start_date > end_date:
        log.info("Gas price data already up to date.")
        conn.close()
        return

    log.info(f"Fetching gas prices from {start_date} to {end_date}...")

    df = fetch_gas_prices(start_date, end_date)

    if df is None or df.empty:
        log.error("No data returned.")
        conn.close()
        return

    log.info(f"Rows fetched: {len(df):,}")
    log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    log.info(f"Price range: ${df['henry_hub_price'].min():.2f} to ${df['henry_hub_price'].max():.2f}")

    validate_gas_data(df)

    inserted, skipped = load_gas_to_db(conn, df)
    log.info(f"Inserted: {inserted:,} | Skipped: {skipped:,}")

    cursor.execute("SELECT COUNT(*), MIN(date), MAX(date), AVG(henry_hub_price) FROM daily_gas_prices")
    total, dmin, dmax, avg = cursor.fetchone()
    log.info(f"Total rows in DB: {total:,} | Range: {dmin} to {dmax} | Avg: ${avg:.2f}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()