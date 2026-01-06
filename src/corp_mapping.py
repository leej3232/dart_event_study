import os
import zipfile
import requests
import pandas as pd
from lxml import etree

CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"

def download_corpcode_zip(api_key: str, out_zip_path: str) -> None:
    params = {"crtfc_key": api_key}
    r = requests.get(CORPCODE_URL, params=params, timeout=60)
    r.raise_for_status()
    with open(out_zip_path, "wb") as f:
        f.write(r.content)

def parse_corpcode_zip(zip_path: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as z:
        xml_name = [n for n in z.namelist() if n.lower().endswith(".xml")][0]
        xml_bytes = z.read(xml_name)

    root = etree.fromstring(xml_bytes)
    rows = []
    for el in root.findall(".//list"):
        corp_code = (el.findtext("corp_code") or "").strip()
        corp_name = (el.findtext("corp_name") or "").strip()
        stock_code = (el.findtext("stock_code") or "").strip()

        # 상장사만 (종목코드 있는 기업)
        if corp_code and stock_code:
            rows.append((corp_code, corp_name, stock_code))

    df = pd.DataFrame(rows, columns=["corp_code", "corp_name", "stock_code"]).drop_duplicates()

    # corp_code는 8자리(앞 0 포함)라 string으로 보정
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)

    return df

def build_mapping(api_key: str, raw_dir="data/raw", processed_dir="data/processed") -> str:
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    zip_path = os.path.join(raw_dir, "corpCode.zip")
    if not os.path.exists(zip_path):
        print("[corpCode] downloading...")
        download_corpcode_zip(api_key, zip_path)

    df = parse_corpcode_zip(zip_path)
    out_path = os.path.join(processed_dir, "corp_mapping.csv")
    df.to_csv(out_path, index=False)
    print(f"[corpCode] saved mapping: {out_path} rows={len(df)}")
    return out_path
