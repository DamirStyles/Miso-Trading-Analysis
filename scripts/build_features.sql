DELETE FROM hourly_features;

INSERT INTO hourly_features (
    timestamp, loadzone_id, da_price, rt_price, spread,
    temp_actual_f, temp_forecast_f, temp_forecast_error_f, dew_point_f, wind_speed_knts,
    forecasted_load_mw, actual_load_mw, load_forecast_error_mw, load_forecast_error_pct,
    gas_price, forced_outages_mw, planned_outages_mw, unplanned_outages_mw,
    derated_outages_mw, total_outages_mw, wind_actual_mwh, wind_forecast_mw,
    wind_forecast_error_mw, binding_constraints_count, max_shadow_price,
    hour, day_of_week, month, quarter, year, is_peak_hour, is_weekend, is_holiday
)
SELECT
    da.timestamp,
    da.loadzone_id,
    da.lmp_price AS da_price,
    rt.lmp_price AS rt_price,
    da.lmp_price - rt.lmp_price AS spread,
    wa.temperature_f AS temp_actual_f,
    wf.temp_forecast_f,
    wa.temperature_f - wf.temp_forecast_f AS temp_forecast_error_f,
    wa.dew_point_f,
    wa.wind_speed_knts,
    hl.forecasted_load_mw,
    hl.actual_load_mw,
    hl.actual_load_mw - hl.forecasted_load_mw AS load_forecast_error_mw,
    CASE
        WHEN hl.forecasted_load_mw > 0
        THEN ROUND((hl.actual_load_mw - hl.forecasted_load_mw) / hl.forecasted_load_mw * 100, 4)
        ELSE NULL
    END AS load_forecast_error_pct,
    gp.henry_hub_price AS gas_price,
    ou.forced_outages_mw,
    ou.planned_outages_mw,
    ou.unplanned_outages_mw,
    ou.derated_outages_mw,
    COALESCE(ou.forced_outages_mw, 0) + COALESCE(ou.unplanned_outages_mw, 0) AS total_outages_mw,
    wnd.wind_actual_mwh,
    wndf.north_mw AS wind_forecast_mw,
    wnd.wind_actual_mwh - wndf.north_mw AS wind_forecast_error_mw,
    tc.binding_constraints_count,
    tc.max_shadow_price,
    CAST(strftime('%H', da.timestamp) AS INTEGER) + 1 AS hour,
    CAST(strftime('%w', da.timestamp) AS INTEGER) AS day_of_week,
    CAST(strftime('%m', da.timestamp) AS INTEGER) AS month,
    CASE
        WHEN CAST(strftime('%m', da.timestamp) AS INTEGER) IN (1, 2, 3) THEN 1
        WHEN CAST(strftime('%m', da.timestamp) AS INTEGER) IN (4, 5, 6) THEN 2
        WHEN CAST(strftime('%m', da.timestamp) AS INTEGER) IN (7, 8, 9) THEN 3
        ELSE 4
    END AS quarter,
    CAST(strftime('%Y', da.timestamp) AS INTEGER) AS year,
    CASE
        WHEN CAST(strftime('%H', da.timestamp) AS INTEGER) BETWEEN 6 AND 21 THEN 1
        ELSE 0
    END AS is_peak_hour,
    CASE
        WHEN CAST(strftime('%w', da.timestamp) AS INTEGER) IN (0, 6) THEN 1
        ELSE 0
    END AS is_weekend,
    CASE
        WHEN strftime('%m-%d', da.timestamp) IN ('01-01', '07-04', '12-25', '12-24', '11-11') THEN 1
        WHEN strftime('%m', da.timestamp) = '11'
         AND CAST(strftime('%w', da.timestamp) AS INTEGER) = 4
         AND CAST(strftime('%d', da.timestamp) AS INTEGER) BETWEEN 22 AND 28 THEN 1
        WHEN strftime('%m', da.timestamp) = '05'
         AND CAST(strftime('%w', da.timestamp) AS INTEGER) = 1
         AND CAST(strftime('%d', da.timestamp) AS INTEGER) BETWEEN 25 AND 31 THEN 1
        WHEN strftime('%m', da.timestamp) = '09'
         AND CAST(strftime('%w', da.timestamp) AS INTEGER) = 1
         AND CAST(strftime('%d', da.timestamp) AS INTEGER) BETWEEN 1 AND 7 THEN 1
        ELSE 0
    END AS is_holiday
FROM hourly_lmp da
LEFT JOIN hourly_lmp rt
    ON da.timestamp = rt.timestamp
    AND da.loadzone_id = rt.loadzone_id
    AND rt.market_id = 2
LEFT JOIN hourly_weather_actual wa ON da.timestamp = wa.timestamp
LEFT JOIN hourly_weather_forecast wf ON wf.ftime = da.timestamp AND wf.runtime = (SELECT MAX(runtime) FROM hourly_weather_forecast WHERE ftime = da.timestamp)
LEFT JOIN hourly_load hl ON da.timestamp = hl.timestamp AND da.loadzone_id = hl.loadzone_id
LEFT JOIN daily_gas_prices gp ON DATE(da.timestamp) = gp.date
LEFT JOIN hourly_outages ou ON DATE(da.timestamp) = DATE(ou.timestamp) AND ou.region = 'North'
LEFT JOIN hourly_wind_actual wnd ON da.timestamp = wnd.timestamp
LEFT JOIN hourly_wind_forecast wndf ON da.timestamp = wndf.timestamp
LEFT JOIN (
    SELECT timestamp, COUNT(*) AS binding_constraints_count, MAX(ABS(shadow_price)) AS max_shadow_price
    FROM transmission_constraints
    GROUP BY timestamp
) tc ON da.timestamp = tc.timestamp
WHERE da.market_id = 1
ORDER BY da.timestamp;

UPDATE hourly_features
SET temp_forecast_f = (
    SELECT wf.temp_forecast_f
    FROM hourly_weather_forecast wf
    WHERE wf.ftime <= hourly_features.timestamp
      AND wf.temp_forecast_f IS NOT NULL
    ORDER BY wf.ftime DESC
    LIMIT 1
)
WHERE temp_forecast_f IS NULL;

UPDATE hourly_features
SET temp_forecast_error_f = temp_actual_f - temp_forecast_f
WHERE temp_forecast_error_f IS NULL
  AND temp_actual_f IS NOT NULL
  AND temp_forecast_f IS NOT NULL;

UPDATE hourly_features
SET temp_7day_rolling_avg = (
    SELECT AVG(temp_actual_f) FROM hourly_features h2
    WHERE h2.timestamp <= hourly_features.timestamp
      AND h2.timestamp > datetime(hourly_features.timestamp, '-7 days')
      AND h2.temp_actual_f IS NOT NULL
);

UPDATE hourly_features
SET spread_7day_rolling_avg = (
    SELECT AVG(spread) FROM hourly_features h2
    WHERE h2.timestamp <= hourly_features.timestamp
      AND h2.timestamp > datetime(hourly_features.timestamp, '-7 days')
      AND h2.spread IS NOT NULL
);

UPDATE hourly_features
SET spread_30day_rolling_avg = (
    SELECT AVG(spread) FROM hourly_features h2
    WHERE h2.timestamp <= hourly_features.timestamp
      AND h2.timestamp > datetime(hourly_features.timestamp, '-30 days')
      AND h2.spread IS NOT NULL
);

UPDATE hourly_features
SET spread_lag_1h = (
    SELECT spread FROM hourly_features h2
    WHERE h2.timestamp = datetime(hourly_features.timestamp, '-1 hour')
);

UPDATE hourly_features
SET spread_lag_24h = (
    SELECT spread FROM hourly_features h2
    WHERE h2.timestamp = datetime(hourly_features.timestamp, '-24 hours')
);

UPDATE hourly_features
SET spread_lag_168h = (
    SELECT spread FROM hourly_features h2
    WHERE h2.timestamp = datetime(hourly_features.timestamp, '-168 hours')
);

UPDATE hourly_features
SET gas_price = (
    SELECT henry_hub_price
    FROM daily_gas_prices
    WHERE date <= DATE(hourly_features.timestamp)
      AND henry_hub_price IS NOT NULL
    ORDER BY date DESC
    LIMIT 1
)
WHERE gas_price IS NULL;