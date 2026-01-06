import os
import pandas as pd

from src.prices_fetcher import fetch_many

def main():
    events_path = "data/processed/events_raw.csv"
    if not os.path.exists(events_path):
        raise RuntimeError("events_raw.csv not found. Run: python run_collect.py")

    events = pd.read_csv(events_path, dtype={"stock_code": "string"})
    if "stock_code" not in events.columns:
        raise RuntimeError("events_raw.csv has no stock_code column")

    # 수집 기간(이벤트 윈도우 여유)
    start, end = "2022-12-01", "2025-01-31"

    codes = events["stock_code"].dropna().astype(str).tolist()
    codes = [c for c in codes if c.strip() != ""]
    codes = sorted(set(codes))

    print(f"[run_prices] unique stock_codes={len(codes)} period={start}~{end}")

    prices = fetch_many(codes, start, end, sleep_sec=0.12)

    if len(prices) == 0:
        print("[run_prices] No price data collected.")
        return

    # 정리: 날짜/코드 타입 통일
    prices["Date"] = pd.to_datetime(prices["Date"])
    prices["stock_code"] = prices["stock_code"].astype(str).str.zfill(6)

    # 필요한 컬럼만(혹시 소스에 따라 달라질 수 있어서 있는 것만)
    preferred = ["Date","stock_code","Open","High","Low","Close","Volume","Change"]
    cols = [c for c in preferred if c in prices.columns] + [c for c in prices.columns if c not in preferred]
    prices = prices[cols].sort_values(["stock_code","Date"]).reset_index(drop=True)

    out_path = "data/processed/prices_daily.csv"
    prices.to_csv(out_path, index=False)
    print(f"[done] saved {out_path} rows={len(prices)} cols={len(prices.columns)}")

    # sanity
    print("[check] head:")
    print(prices.head(3))
    print("[check] date range:", prices["Date"].min(), "~", prices["Date"].max())

if __name__ == "__main__":
    main()
