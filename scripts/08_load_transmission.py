import os
import sqlite3
import logging
import re
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\08_load_transmission.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH      = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
TX_FOLDER    = r"C:\文件\MISO_Trading_Analysis\data\raw\Transmission Constraints 2023-2025"


def parse_shadow_price(value):
    """
    Convert shadow price string to float.
    Input formats: '($72.31)' = -72.31, '$50.00' = 50.00, '72.31' = 72.31
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "").replace("$", "").replace(",", "").strip()
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def load_transmission_to_db(conn, df, filename):
    cursor = conn.cursor()
    rows_inserted = 0
    rows_skipped  = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO transmission_constraints
                    (market_date, hour_of_occurrence, timestamp,
                     flowgate_nercid, constraint_id, shadow_price,
                     branch_name, contingency_desc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["market_date"],
                row["hour_of_occurrence"],
                row["timestamp"],
                row["flowgate_nercid"] if pd.notna(row["flowgate_nercid"]) else None,
                row["constraint_id"]   if pd.notna(row["constraint_id"])   else None,
                row["shadow_price"]    if pd.notna(row["shadow_price"])    else None,
                str(row["branch_name"])[:500] if pd.notna(row["branch_name"]) else None,
                str(row["contingency_desc"])[:500] if pd.notna(row["contingency_desc"]) else None,
            ))
            if cursor.rowcount == 1:
                rows_inserted += 1
            else:
                rows_skipped += 1
        except Exception as e:
            log.error(f"  Row insert error: {e}")

    conn.commit()
    return rows_inserted, rows_skipped


def parse_transmission_file(filepath):
    filename = os.path.basename(filepath)

    try:
        # Skip 2 header rows (title + publish date)
        df = pd.read_csv(filepath, skiprows=2, low_memory=False, on_bad_lines='skip')
    except Exception as e:
        log.error(f"  Failed to read {filename}: {e}")
        return None

    # Expected columns:
    # Market Date, Flowgate NERCID, Constraint_ID, Constraint Name,
    # Branch Name, Contingency Description, Hour of Occurrence,
    # Preliminary Shadow Price, ...

    df.columns = [c.strip() for c in df.columns]

    # Rename to standard names
    rename_map = {
        "Market Date":               "market_date",
        "Flowgate NERCID":           "flowgate_nercid",
        "Constraint_ID":             "constraint_id",
        "Constraint Name":           "constraint_name",
        "Branch Name ( Branch Type / From CA / To CA )": "branch_name",
        "Contingency Description":   "contingency_desc",
        "Hour of Occurrence":        "hour_of_occurrence",
        "Preliminary Shadow Price":  "shadow_price_raw",
    }
    df = df.rename(columns=rename_map)

    # Drop rows missing market date
    df = df.dropna(subset=["market_date"])
    df = df[df["market_date"] != "Market Date"].copy()

    # Parse market date
    df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce")
    df = df.dropna(subset=["market_date"])

    # Build timestamp from market_date + hour_of_occurrence (format: HH:MM)
    def build_timestamp(row):
        try:
            time_str = str(row["hour_of_occurrence"]).strip()
            hour = int(time_str.split(":")[0])
            return row["market_date"].replace(hour=hour, minute=0, second=0)
        except:
            return row["market_date"]

    df["timestamp"] = df.apply(build_timestamp, axis=1)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["market_date"] = df["market_date"].dt.strftime("%Y-%m-%d")

    # Parse shadow price
    df["shadow_price"] = df["shadow_price_raw"].apply(parse_shadow_price)

    # Convert constraint_id to string
    df["constraint_id"] = df["constraint_id"].astype(str).str.strip()
    df["flowgate_nercid"] = df["flowgate_nercid"].astype(str).str.strip()

    return df


def main():
    log.info("=" * 60)
    log.info("08_load_transmission.py — MISO Transmission Constraints Loader")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    files = sorted([f for f in os.listdir(TX_FOLDER) if f.endswith(".csv")])
    log.info(f"Found {len(files)} transmission constraint files")

    total_inserted = 0
    total_skipped  = 0

    for filename in files:
        filepath = os.path.join(TX_FOLDER, filename)
        log.info(f"Loading {filename}...")

        df = parse_transmission_file(filepath)

        if df is None or df.empty:
            log.warning(f"  Skipping {filename} — no usable data")
            continue

        log.info(f"  Parsed {len(df):,} rows")

        inserted, skipped = load_transmission_to_db(conn, df, filename)
        total_inserted += inserted
        total_skipped  += skipped
        log.info(f"  Inserted: {inserted:,} | Skipped: {skipped:,}")

    log.info(f"TOTAL — Inserted: {total_inserted:,} | Skipped: {total_skipped:,}")

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), MIN(market_date), MAX(market_date) FROM transmission_constraints")
    total, date_min, date_max = cursor.fetchone()
    log.info(f"Total rows in transmission_constraints: {total:,}")
    log.info(f"Date range: {date_min} to {date_max}")

    cursor.execute("""
        SELECT market_date, hour_of_occurrence, shadow_price, branch_name
        FROM transmission_constraints
        ORDER BY ABS(shadow_price) DESC
        LIMIT 5
    """)
    log.info("Top 5 highest shadow price constraints:")
    for row in cursor.fetchall():
        log.info(f"  {row}")

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()