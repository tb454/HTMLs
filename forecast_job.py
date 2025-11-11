import os, asyncio, asyncpg, math, warnings
import numpy as np
import pandas as pd
from datetime import date, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
warnings.filterwarnings("ignore")

DATABASE_URL = os.getenv("DATABASE_URL")

FORECAST_SYMBOLS = [
    "BR-CU#1", "BR-CU-BARLEY", "BR-AL6063-OLD", "BR-ICW1-THHN80", "BR-ZN-DIECAST"
]
HORIZONS = [7, 30, 90]

EXOG_COLS = ["diesel_usd_gal","truck_spot_idx","rail_spot_idx",
             "export_premium","ism_pmi","usd_index"]

def make_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("dt").copy()
    df["close_lag1"] = df["close_price"].shift(1)
    df["close_lag2"] = df["close_price"].shift(2)
    df["close_lag5"] = df["close_price"].shift(5)
    df["close_lag7"] = df["close_price"].shift(7)
    df["close_lag14"] = df["close_price"].shift(14)
    df["sma_5"] = df["close_price"].rolling(5).mean()
    df["sma_10"] = df["close_price"].rolling(10).mean()
    df["sma_20"] = df["close_price"].rolling(20).mean()
    df["vol_5"] = df["close_price"].pct_change().rolling(5).std()
    return df

async def fetch_symbol_history(conn, symbol: str) -> pd.DataFrame:
    q = """SELECT dt, close_price FROM bridge_index_history
           WHERE symbol=$1 ORDER BY dt"""
    rows = await conn.fetch(q, symbol)
    if not rows:
        return pd.DataFrame(columns=["dt","close_price"])
    return pd.DataFrame(rows, columns=["dt","close_price"])

async def fetch_exog(conn, start_dt: date, end_dt: date) -> pd.DataFrame:
    q = """SELECT dt, diesel_usd_gal, truck_spot_idx, rail_spot_idx,
                  export_premium, ism_pmi, usd_index
           FROM exogenous_daily
           WHERE dt BETWEEN $1 AND $2
           ORDER BY dt"""
    rows = await conn.fetch(q, start_dt, end_dt)
    if not rows:
        # empty frame with dt for merge
        dts = pd.date_range(start_dt, end_dt, freq="D")
        df = pd.DataFrame({"dt": dts.date})
        for c in EXOG_COLS: df[c] = np.nan
        return df
    return pd.DataFrame(rows, columns=["dt"] + EXOG_COLS)

def fit_sarimax(endog: pd.Series, exog: pd.DataFrame):
    # Reasonable starting order; can grid-search later
    order = (1,1,1)
    seasonal_order = (0,0,0,0)  # daily, no strong weekly seasonality in metals; adjust if needed
    model = SARIMAX(endog, exog=exog, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    res = model.fit(disp=False)
    return res

def rolling_backtest(y: pd.Series, ex: pd.DataFrame, window: int = 60):
    if len(y) < window + 7:  # need enough data
        return None, None
    ys = y[-window:]
    xs = ex.iloc[-window:]
    # one-step ahead walk-forward
    preds, actuals = [], []
    for i in range(7, len(ys)):  # simple backtest window
        train_y = ys[:i]
        train_x = xs.iloc[:i]
        test_x  = xs.iloc[i:i+1]
        res = fit_sarimax(train_y, train_x)
        pred = res.get_forecast(steps=1, exog=test_x).predicted_mean.iloc[0]
        preds.append(float(pred))
        actuals.append(float(ys.iloc[i]))
    if not preds:
        return None, None
    err = np.abs(np.array(preds) - np.array(actuals))
    mae = float(np.mean(err))
    mape = float(np.mean(err / np.maximum(np.array(actuals), 1e-6))) * 100.0
    return mae, mape

async def save_run(conn, model_name, symbol, train_start, train_end, features_used, mae, mape):
    q = """INSERT INTO model_runs (model_name, symbol, train_start, train_end, features_used, backtest_mae, backtest_mape)
           VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id"""
    rid = await conn.fetchval(q, model_name, symbol, train_start, train_end, features_used, mae, mape)
    return rid

async def save_forecasts(conn, symbol, horizon, fcs, conf, model_name, run_id):
    # fcs: list of (dt, pred), conf: list of (lo, hi)
    q = """INSERT INTO bridge_forecasts (symbol, horizon_days, forecast_date, predicted_price, conf_low, conf_high, model_name, run_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (symbol, horizon_days, forecast_date, model_name)
           DO UPDATE SET predicted_price=EXCLUDED.predicted_price,
                         conf_low=EXCLUDED.conf_low,
                         conf_high=EXCLUDED.conf_high,
                         run_id=EXCLUDED.run_id"""
    for (fdt, pred), (lo, hi) in zip(fcs, conf):
        await conn.execute(q, symbol, horizon, fdt, float(pred),
                           float(lo) if lo is not None else None,
                           float(hi) if hi is not None else None,
                           model_name, run_id)

async def run_all():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with pool.acquire() as conn:
        for symbol in FORECAST_SYMBOLS:
            hist = await fetch_symbol_history(conn, symbol)
            if hist.empty or len(hist) < 60:
                continue
            start_dt, end_dt = hist["dt"].min(), hist["dt"].max()
            exog = await fetch_exog(conn, start_dt, end_dt)

            df = hist.merge(exog, on="dt", how="left")
            df = make_features(df).dropna().reset_index(drop=True)
            if df.empty:
                continue

            y = df["close_price"]
            X = df[EXOG_COLS].fillna(method="ffill").fillna(method="bfill")  # simple fill
            mae, mape = rolling_backtest(y, X, window=min(120, len(df)-7))
            rid = await save_run(conn, "SARIMAX_exog_v1", symbol,
                                 start_dt, end_dt,
                                 features_used={"exog": EXOG_COLS}, 
                                 mae=mae if mae is not None else None,
                                 mape=mape if mape is not None else None)

            # fit on full sample
            model = fit_sarimax(y, X)
            last_dt = df["dt"].iloc[-1]
            forecasts = []
            conf_ints = []

            # Build exog for future horizons (hold last known values)
            last_ex = X.iloc[[-1]].values
            for H in HORIZONS:
                steps = H
                future_ex = np.repeat(last_ex, steps, axis=0)
                pred_res = model.get_forecast(steps=steps, exog=future_ex)
                mean = pred_res.predicted_mean.values
                ci = pred_res.conf_int(alpha=0.2)  # ~80% CI
                lows = ci.iloc[:,0].values
                highs = ci.iloc[:,1].values
                fcs = []
                con = []
                for i in range(steps):
                    fdt = last_dt + timedelta(days=i+1)
                    fcs.append((fdt, float(mean[i])))
                    con.append((float(lows[i]), float(highs[i])))
                await save_forecasts(conn, symbol, H, fcs, con, "SARIMAX_exog_v1", rid)
    await pool.close()

if __name__ == "__main__":
    asyncio.run(run_all())
