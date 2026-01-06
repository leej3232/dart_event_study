import pandas as pd
from typing import Optional

def load_events(path: str) -> pd.DataFrame:
    events = pd.read_csv(
        path,
        dtype={"corp_code": "string", "stock_code": "string", "rcept_no": "string", "rcept_dt": "string"},
    )
    events["stock_code"] = events["stock_code"].astype("string").str.zfill(6)

    # rcept_dt: YYYYMMDD
    events["event_date"] = pd.to_datetime(events["rcept_dt"], format="%Y%m%d", errors="coerce")
    events = events.dropna(subset=["event_date", "stock_code"]).reset_index(drop=True)
    return events

def load_prices(path: str) -> pd.DataFrame:
    prices = pd.read_csv(
        path,
        dtype={"stock_code": "string"},
        parse_dates=["Date"],
    )
    prices["stock_code"] = prices["stock_code"].astype("string").str.zfill(6)
    prices = prices.sort_values(["stock_code", "Date"]).reset_index(drop=True)
    return prices

def next_trading_day(prices_one: pd.DataFrame, dt: pd.Timestamp) -> Optional[pd.Timestamp]:
    """Return the first trading day >= dt for that stock."""
    idx = prices_one["Date"].searchsorted(dt)
    if idx >= len(prices_one):
        return None
    return prices_one.iloc[idx]["Date"]

def build_event_panel(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    pre: int = 20,
    post: int = 20,
) -> pd.DataFrame:
    by_code = {code: df.reset_index(drop=True) for code, df in prices.groupby("stock_code", sort=False)}

    rows = []
    total = len(events)

    for i, ev in events.iterrows():
        code = str(ev["stock_code"]).zfill(6)
        if code not in by_code:
            continue

        p = by_code[code]
        anchor = next_trading_day(p, ev["event_date"])
        if anchor is None:
            continue

        anchor_idx = int(p["Date"].searchsorted(anchor))
        start_idx = anchor_idx - pre
        end_idx = anchor_idx + post

        if start_idx < 0 or end_idx >= len(p):
            continue

        window = p.iloc[start_idx : end_idx + 1].copy()
        window["tau"] = range(-pre, post + 1)

        window["rcept_no"] = ev.get("rcept_no", None)
        window["rcept_dt"] = ev.get("rcept_dt", None)
        window["event_date"] = ev["event_date"]
        window["event_trade_date"] = anchor
        window["corp_code"] = ev.get("corp_code", None)
        window["corp_name"] = ev.get("corp_name", None)
        window["report_nm"] = ev.get("report_nm", None)
        window["pblntf_ty"] = ev.get("pblntf_ty", None)
        window["pblntf_detail_ty"] = ev.get("pblntf_detail_ty", None)

        # 기준가(이벤트 거래일 종가)로 정규화
        p0 = float(p.iloc[anchor_idx]["Close"])
        window["close_norm"] = window["Close"] / p0

        rows.append(window)

        if (i + 1) % 50 == 0:
            print(f"[panel] {i+1}/{total} events processed")

    if not rows:
        return pd.DataFrame()

    panel = pd.concat(rows, ignore_index=True)

    # Close 기준 단순 수익률
    panel = panel.sort_values(["rcept_no", "stock_code", "Date"]).reset_index(drop=True)
    panel["ret_close"] = panel.groupby(["rcept_no", "stock_code"])["Close"].pct_change()

    return panel
