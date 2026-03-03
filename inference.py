"""
MISO DA-RT Spread Forecasting — Hourly Inference Script
--------------------------------------------------------
Run this before 10am CT daily to generate virtual bid signals
for the Day-Ahead market close.

Requirements:
  - lstm_model.pth   (saved after training)
  - scaler_X.pkl     (saved after training)
  - scaler_y.pkl     (saved after training)

Does NOT retrain. Does NOT refit scalers.
Logs every prediction to inference_log table in the DB.
"""

import sqlite3
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import joblib
from datetime import datetime, timedelta

# ── Paths ────────────────────────────────────────────────────────────────────
DB_PATH       = r"C:\文件\MISO_Trading_Analysis\database\miso_trading.db"
MODEL_PATH    = r"C:\文件\MISO_Trading_Analysis\outputs\lstm_model.pth"
SCALER_X_PATH = r"C:\文件\MISO_Trading_Analysis\outputs\scaler_X.pkl"
SCALER_Y_PATH = r"C:\文件\MISO_Trading_Analysis\outputs\scaler_y.pkl"

# ── Config (must match training exactly) ─────────────────────────────────────
FEATURE_COLS = [
    'hour', 'day_of_week', 'month', 'quarter',
    'is_peak_hour', 'is_weekend', 'is_holiday',
    'temp_forecast_f', 'forecasted_load_mw', 'wind_forecast_mw',
    'gas_price',
    'forced_outages_mw', 'planned_outages_mw',
    'unplanned_outages_mw', 'total_outages_mw',
    'binding_constraints_count', 'max_shadow_price',
    'spread_lag_1h', 'spread_lag_24h', 'spread_lag_168h',
    'spread_7day_rolling_avg', 'spread_30day_rolling_avg',
    'temp_7day_rolling_avg',
]
SEQUENCE_LENGTH = 168
HIDDEN_SIZE     = 128
NUM_LAYERS      = 2
DROPOUT         = 0.2
POSITION_MW     = 25
THRESHOLD       = 2.0

# ── Model definition (must match training exactly) ────────────────────────────
class SpreadLSTM(nn.Module):
    def __init__(self, input_size, hidden_size=128, num_layers=2, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
            batch_first=True,
        )
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, 1),
        )

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])

# ── Create inference_log table if it doesn't exist ───────────────────────────
def init_log_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inference_log (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp     TEXT NOT NULL,
            target_hour       TEXT NOT NULL,
            predicted_spread  REAL NOT NULL,
            signal            TEXT NOT NULL,
            threshold         REAL NOT NULL,
            position_mw       REAL NOT NULL,
            expected_pnl      REAL,
            actual_spread     REAL,
            actual_pnl        REAL
        )
    """)
    conn.commit()

# ── Load artifacts ────────────────────────────────────────────────────────────
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

model = SpreadLSTM(input_size=len(FEATURE_COLS), hidden_size=HIDDEN_SIZE,
                   num_layers=NUM_LAYERS, dropout=DROPOUT).to(device)
model.load_state_dict(torch.load(MODEL_PATH, map_location=device))
model.eval()

scaler_X = joblib.load(SCALER_X_PATH)
scaler_y = joblib.load(SCALER_Y_PATH)

print(f"Model and scalers loaded. Running inference at {datetime.now():%Y-%m-%d %H:%M}")

# ── Pull latest data from DB ──────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
init_log_table(conn)

df = pd.read_sql_query(f"""
    SELECT * FROM hourly_features
    ORDER BY timestamp DESC
    LIMIT {SEQUENCE_LENGTH + 24}
""", conn)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)
df = df.dropna(subset=FEATURE_COLS)

if len(df) < SEQUENCE_LENGTH:
    conn.close()
    raise ValueError(f"Not enough clean rows: need {SEQUENCE_LENGTH}, got {len(df)}")

# ── Build sequence & predict ──────────────────────────────────────────────────
X_raw    = df[FEATURE_COLS].values[-SEQUENCE_LENGTH:]
X_scaled = scaler_X.transform(X_raw)
X_tensor = torch.FloatTensor(X_scaled).unsqueeze(0).to(device)

with torch.no_grad():
    y_pred_scaled = model(X_tensor).cpu().numpy()

y_pred      = scaler_y.inverse_transform(y_pred_scaled).flatten()[0]
target_hour = df['timestamp'].iloc[-1] + timedelta(hours=1)

# ── Generate signal ───────────────────────────────────────────────────────────
if y_pred > THRESHOLD:
    signal       = 'DEC'
    rationale    = f"Predict DA > RT by ${y_pred:.2f}/MWh — submit DEC virtual bid"
    expected_pnl = y_pred * POSITION_MW
elif y_pred < -THRESHOLD:
    signal       = 'INC'
    rationale    = f"Predict RT > DA by ${abs(y_pred):.2f}/MWh — submit INC virtual bid"
    expected_pnl = abs(y_pred) * POSITION_MW
else:
    signal       = 'NO TRADE'
    rationale    = f"Predicted spread ${y_pred:.2f}/MWh within +/-${THRESHOLD} threshold"
    expected_pnl = None

# ── Log to DB ─────────────────────────────────────────────────────────────────
conn.execute("""
    INSERT INTO inference_log
        (run_timestamp, target_hour, predicted_spread, signal,
         threshold, position_mw, expected_pnl, actual_spread, actual_pnl)
    VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
""", (
    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    target_hour.strftime('%Y-%m-%d %H:%M:%S'),
    round(float(y_pred), 4),
    signal,
    THRESHOLD,
    POSITION_MW,
    round(float(expected_pnl), 2) if expected_pnl is not None else None,
))
conn.commit()
conn.close()

# ── Print output ──────────────────────────────────────────────────────────────
print(f"\n{'─'*50}")
print(f"  Target hour:       {target_hour:%Y-%m-%d %H:00 CT}")
print(f"  Predicted spread:  ${y_pred:.2f}/MWh  (DA - RT)")
print(f"  Signal:            {signal}")
print(f"  Rationale:         {rationale}")
print(f"  Position size:     {POSITION_MW} MW")
if expected_pnl is not None:
    print(f"  Expected P&L:      ${expected_pnl:,.0f} (if prediction correct)")
print(f"{'─'*50}")
print(f"  Logged to inference_log in DB.\n")
