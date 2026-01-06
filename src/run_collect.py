import os
import pandas as pd
from dotenv import load_dotenv

from src.corp_mapping import build_mapping
from src.dart_fetcher import collect_by_month

def main():
    load_dotenv()
    api_key = os.getenv("OPENDART_API_KEY")
    if not api_key:
        raise RuntimeError("OPENDART_API_KEY is missing. Put it in .env")

    start, end = "2023-01-01", "2024-12-31"
    corp_cls = "Y"
    pblntf_detail_ty = "A003"

    mapping_path = build_mapping(api_key)
    mapping = pd.read_csv(mapping_path)

    dart_df = collect_by_month(api_key, start, end, corp_cls=corp_cls, pblntf_detail_ty=pblntf_detail_ty)
    print(f"[list] collected rows={len(dart_df)} cols={len(dart_df.columns)}")

    if len(dart_df) == 0:
        print("No data collected. Check API key / parameters.")
        return

    if "corp_code" in dart_df.columns:
        dart_df = dart_df.merge(mapping, on="corp_code", how="left")

    preferred = [
        "rcept_no", "rcept_dt", "corp_code", "stock_code", "corp_name",
        "report_nm", "pblntf_ty", "pblntf_detail_ty"
    ]
    cols = [c for c in preferred if c in dart_df.columns] + [c for c in dart_df.columns if c not in preferred]
    dart_df = dart_df[cols]

    os.makedirs("data/processed", exist_ok=True)
    out_path = "data/processed/events_raw.csv"
    dart_df.to_csv(out_path, index=False)
    print(f"[done] saved {out_path}")

    print("[check] head:")
    print(dart_df.head(3))
    if "stock_code" in dart_df.columns:
        print("[check] null stock_code ratio:", dart_df["stock_code"].isna().mean())

if __name__ == "__main__":
    main()
