import sqlite3
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(r"C:\文件\MISO_Trading_Analysis\logs\09_build_features.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH  = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
SQL_PATH = r"C:\文件\MISO_Trading_Analysis\scripts\build_features.sql"


def run_step(conn, sql, description):
    """Execute a SQL statement and log time taken."""
    cursor = conn.cursor()
    log.info(f"Running: {description}...")
    start = time.time()
    cursor.execute(sql)
    conn.commit()
    elapsed = time.time() - start
    log.info(f"  Done in {elapsed:.1f}s | Rows affected: {cursor.rowcount}")
    return cursor.rowcount


def validate_features(conn):
    """Run post-build validation checks on hourly_features."""
    cursor = conn.cursor()
    log.info("Running validation checks...")

    cursor.execute("SELECT COUNT(*) FROM hourly_features")
    total = cursor.fetchone()[0]
    log.info(f"  Total rows: {total:,}")

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM hourly_features")
    date_min, date_max = cursor.fetchone()
    log.info(f"  Date range: {date_min} to {date_max}")

    # Check NULL rates for key columns
    key_cols = ["da_price", "rt_price", "spread", "temp_actual_f", "temp_forecast_f",
                "forecasted_load_mw", "gas_price", "forced_outages_mw"]
    for col in key_cols:
        cursor.execute(f"SELECT ROUND(100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) FROM hourly_features")
        null_pct = cursor.fetchone()[0]
        status = "OK" if null_pct < 5 else "WARNING"
        log.info(f"  {col} NULL rate: {null_pct}%  [{status}]")

    # Spread statistics
    cursor.execute("""
        SELECT
            ROUND(AVG(spread), 2),
            ROUND(MIN(spread), 2),
            ROUND(MAX(spread), 2),
            ROUND(AVG(CASE WHEN spread > 0 THEN 1.0 ELSE 0.0 END) * 100, 1)
        FROM hourly_features
        WHERE spread IS NOT NULL
    """)
    avg_sp, min_sp, max_sp, win_rate = cursor.fetchone()
    log.info(f"  Spread — Avg: ${avg_sp}, Min: ${min_sp}, Max: ${max_sp}, Win rate: {win_rate}%")

    # Quarterly summary
    log.info("  Quarterly spread summary:")
    cursor.execute("""
        SELECT year, quarter, COUNT(*) as hours,
               ROUND(AVG(spread), 2) as avg_spread,
               ROUND(AVG(CASE WHEN spread > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) as win_rate
        FROM hourly_features
        WHERE spread IS NOT NULL
        GROUP BY year, quarter
        ORDER BY year, quarter
    """)
    for row in cursor.fetchall():
        log.info(f"    {row[0]} Q{row[1]}: {row[2]} hrs | Avg spread: ${row[3]} | Win rate: {row[4]}%")

    # Check lag features populated
    cursor.execute("SELECT COUNT(*) FROM hourly_features WHERE spread_lag_1h IS NOT NULL")
    lag_count = cursor.fetchone()[0]
    log.info(f"  Rows with lag_1h populated: {lag_count:,}")

    cursor.execute("SELECT COUNT(*) FROM hourly_features WHERE spread_7day_rolling_avg IS NOT NULL")
    roll_count = cursor.fetchone()[0]
    log.info(f"  Rows with 7day rolling avg populated: {roll_count:,}")


def main():

    log.info("09_build_features.py — Feature Engineering Pipeline")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Read SQL file
    with open(SQL_PATH, 'r') as f:
        sql_content = f.read()

    # Split into individual statements
    statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")

    log.info(f"Executing {len(statements)} SQL statements...")

    descriptions = [
        "Clear existing hourly_features data",
        "Insert base features (join all fact tables)",
        "Forward-fill temp_forecast_f for non-MOS hours",
        "Recalculate temp_forecast_error_f after forward fill",
        "Update 7-day rolling temperature average",
        "Update 7-day rolling spread average",
        "Update 30-day rolling spread average",
        "Update spread lag 1h",
        "Update spread lag 24h",
        "Update spread lag 168h",
        "Forward-fill gas prices for weekends and holidays",
    ]

    total_start = time.time()

    for i, stmt in enumerate(statements):
        desc = descriptions[i] if i < len(descriptions) else f"Statement {i+1}"
        run_step(conn, stmt, desc)

    total_elapsed = time.time() - total_start
    log.info(f"All statements completed in {total_elapsed:.1f}s")

    validate_features(conn)

    conn.close()
    log.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()