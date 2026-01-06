import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

LIST_URL = "https://opendart.fss.or.kr/api/list.json"

def yyyymmdd(s: str) -> str:
    return s.replace("-", "")

def month_chunks(start: str, end: str):
    """Yield (bgn, end) per month as YYYY-MM-DD strings."""
    cur = datetime.strptime(start, "%Y-%m-%d").replace(day=1)
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    while cur <= end_dt:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)  # next month 1st
        month_end = min(nxt - timedelta(days=1), end_dt)
        yield cur.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")
        cur = nxt

def fetch_list_once(
    api_key: str,
    bgn_de: str,
    end_de: str,
    corp_cls: str = "Y",
    pblntf_detail_ty: str = "A003",
    page_count: int = 100,
    cache_dir: str = "data/raw/dart_list",
) -> pd.DataFrame:
    os.makedirs(cache_dir, exist_ok=True)
    cache_name = f"list_{corp_cls}_{pblntf_detail_ty}_{yyyymmdd(bgn_de)}_{yyyymmdd(end_de)}.json"
    cache_path = os.path.join(cache_dir, cache_name)

    if os.path.exists(cache_path):
        return pd.read_json(cache_path)

    params = {
        "crtfc_key": api_key,
        "bgn_de": yyyymmdd(bgn_de),
        "end_de": yyyymmdd(end_de),
        "corp_cls": corp_cls,                 # Y=KOSPI, K=KOSDAQ
        "pblntf_detail_ty": pblntf_detail_ty, # A003=분기보고서
        "page_count": page_count
    }

    r = requests.get(LIST_URL, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()

    if js.get("status") != "000":
        msg = js.get("message", "")
        print(f"[list] skip {bgn_de}~{end_de} status={js.get('status')} message={msg}")
        return pd.DataFrame()

    df = pd.DataFrame(js.get("list", []))

    # corp_code는 8자리 string으로 맞추기(앞 0 보존)
    if "corp_code" in df.columns:
        df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)

    df.to_json(cache_path, orient="records", force_ascii=False)
    time.sleep(0.25)  # 호출 과다 방지
    return df

def collect_by_month(
    api_key: str,
    start: str,
    end: str,
    corp_cls: str = "Y",
    pblntf_detail_ty: str = "A003",
) -> pd.DataFrame:
    dfs = []
    for bgn, ed in month_chunks(start, end):
        df = fetch_list_once(
            api_key,
            bgn,
            ed,
            corp_cls=corp_cls,
            pblntf_detail_ty=pblntf_detail_ty,
        )
        if len(df) > 0:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)

    # rcept_no 기준 중복 제거
    if "rcept_no" in out.columns:
        out = out.drop_duplicates(subset=["rcept_no"])

    return out
