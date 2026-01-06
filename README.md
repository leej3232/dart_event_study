
# DART Event Study (KOSPI Quarterly Filings, 2023–2024)

An end-to-end mini event-study pipeline for Korean listed equities:
- Collect quarterly filing events from OpenDART
- Fetch daily stock prices
- Build an event window panel (tau = -20…+20 trading days)
- Compute CAR summaries and aggregate AAR/CAAR
- Save reproducible outputs + plots

## Why this project?
Korea’s DART filings are a structured, public signal stream.  
This project turns filings into a research-ready dataset and event-study outputs with a clean, reproducible pipeline.

---

## Data Sources
- **OpenDART**: corporate code mapping + filing list (quarterly report, A003)
- **FinanceDataReader**: daily OHLCV for listed stocks

> Note: API key is stored locally in `.env` and never committed.

---

## Outputs (generated)
- `data/processed/events_raw.csv`  
  Filing events: receipt no/date, corp/stock identifiers, report name
- `data/processed/prices_daily.csv`  
  Daily OHLCV prices for relevant stocks
- `data/processed/event_panel.csv`  
  Long-format panel: each event × tau(-20..20) × daily stock row
- `data/processed/event_summary.csv`  
  Event-level CAR summaries: CAR(-1,+1), CAR(-5,+5), CAR(-20,+20)
- `data/processed/caar.csv`  
  Aggregate AAR/CAAR by tau
- `reports/figures/aar.png`, `reports/figures/caar.png`

---

## Repo Structure
dart_event_study/
src/
corp_mapping.py # OpenDART corpCode mapping -> corp_code, stock_code
dart_fetcher.py # OpenDART list API monthly collection
event_panel.py # event window panel builder (-20..+20 trading days)
prices_fetcher.py # FinanceDataReader price collector
data/
raw/ # cached API files (not committed)
processed/ # generated CSVs (not committed)
reports/
figures/ # AAR/CAAR plots
run_collect.py
run_prices.py
run_panel.py
run_summary.py
run_plot.py

yaml
코드 복사

---

## Quickstart (Mac, Python venv)
### 1) Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install finance-datareader matplotlib
2) Add OpenDART API key
Create .env in the project root:

bash
코드 복사
OPENDART_API_KEY=YOUR_KEY_HERE
3) Run pipeline
bash
코드 복사
python -u run_collect.py    # events_raw.csv
python -u run_prices.py     # prices_daily.csv
python -u run_panel.py      # event_panel.csv (tau=-20..20)
python -u run_summary.py    # event_summary.csv + caar.csv
python -u run_plot.py       # aar.png + caar.png
Method Summary
Event definition: DART filings filtered to KOSPI (corp_cls=Y) and quarterly reports (A003) in 2023–2024.

Event date alignment: If a filing date is a non-trading day, use the next trading day for that stock.

Event window: tau ∈ [-20, +20] trading days.

Returns: simple daily close-to-close return (pct_change).

CAR: sum of returns within the window (NaN filled as 0 for aggregation).

AAR/CAAR: average return per tau and cumulative average return.

Results Snapshot
Number of complete events with full window: 550

Window length: 41 trading days (tau -20..+20)

Example aggregate: CAAR at tau=+20 is reported in data/processed/caar.csv

Plots:

reports/figures/aar.png

reports/figures/caar.png

Limitations & Next Steps
Market adjustment (e.g., subtract KOSPI index return) to compute abnormal returns

Filtering by sector/market cap/liquidity

Robustness checks: winsorization, log returns, alternative windows

Regression-based event study (e.g., market model, Fama-French style factors if available)

Notes
This repository is designed to be reproducible and safe:

.env is excluded via .gitignore

raw caches / processed CSVs can be excluded depending on storage preference
