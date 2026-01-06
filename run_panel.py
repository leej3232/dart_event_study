import os
import pandas as pd
from src.event_panel import load_events, load_prices, build_event_panel

def main():
    events_path = "data/processed/events_raw.csv"
    prices_path = "data/processed/prices_daily.csv"

    if not os.path.exists(events_path):
        raise RuntimeError("events_raw.csv not found. Run: python run_collect.py")
    if not os.path.exists(prices_path):
        raise RuntimeError("prices_daily.csv not found. Run: python run_prices.py")

    events = load_events(events_path)
    prices = load_prices(prices_path)

    print(f"[run_panel] events={len(events)} prices_rows={len(prices)} unique_codes={prices['stock_code'].nunique()}")

    panel = build_event_panel(events, prices, pre=20, post=20)

    if len(panel) == 0:
        print("[run_panel] panel is empty")
        return

    os.makedirs("data/processed", exist_ok=True)
    out_path = "data/processed/event_panel.csv"
    panel.to_csv(out_path, index=False)
    print(f"[done] saved {out_path} rows={len(panel)} cols={len(panel.columns)}")

    # sanity
    print(panel.head(3)[["rcept_no","stock_code","event_trade_date","tau","Date","Close","ret_close","close_norm"]])

if __name__ == "__main__":
    main()
