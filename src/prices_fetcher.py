import time
import pandas as pd
import FinanceDataReader as fdr

def normalize_stock_code(code: str) -> str:
    # 6자리 종목코드로 보정
    s = str(code).strip()
    s = s.split(".")[0]  # 혹시 005930.0 같은 형태 방지
    return s.zfill(6)

def fetch_one(code: str, start: str, end: str) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
    Date, Open, High, Low, Close, Volume, Change
    """
    code = normalize_stock_code(code)
    df = fdr.DataReader(code, start, end)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df.reset_index()  # Date column
    df["stock_code"] = code
    return df

def fetch_many(stock_codes, start: str, end: str, sleep_sec: float = 0.1) -> pd.DataFrame:
    out = []
    stock_codes = sorted({normalize_stock_code(c) for c in stock_codes if pd.notna(c)})

    for i, code in enumerate(stock_codes, start=1):
        try:
            df = fetch_one(code, start, end)
            if len(df) > 0:
                out.append(df)
            print(f"[prices] {i}/{len(stock_codes)} {code} rows={len(df)}")
        except Exception as e:
            print(f"[prices] {i}/{len(stock_codes)} {code} ERROR: {e}")
        time.sleep(sleep_sec)

    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)
