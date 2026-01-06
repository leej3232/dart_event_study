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

    # Week1: 2023~2024, KOSPI(Y), 분기보고서(A003)
    start, end = "2023-01-01", "2024-12-31"
    corp_cls = "Y"
    pblntf_detail_ty = "A003"

    # 1) corp mapping 생성/로드
    mapping_path = build_mapping(api_key)
    mapping = pd.read_csv(mapping_path)

    # 타입/자리수 통일 (merge 오류 + 앞 0 날아가는 문제 방지)
    mapping["corp_code"] = mapping["corp_code"].astype(str).str.zfill(8)
    mapping["stock_code"] = mapping["stock_code"].astype(str).str.zfill(6)

    # 2) 공시 수집
    dart_df = collect_by_month(api_key, start, end, corp_cls=corp_cls, pblntf_detail_ty=pblntf_detail_ty)
    print(f"[list] collected rows={len(dart_df)} cols={len(dart_df.columns)}")

    if len(dart_df) == 0:
        print("No data collected. Check API key / parameters.")
        return

    dart_df["corp_code"] = dart_df["corp_code"].astype(str).str.zfill(8)

    # 3) stock_code, corp_name 붙이기 (list에 없는 경우 대비)
    dart_df = dart_df.merge(mapping, on="corp_code", how="left", suffixes=("_list", "_map"))

    # 4) 컬럼 정리: 최종 corp_name/stock_code 결정
    # list쪽이 있으면 우선, 없으면 mapping값 사용
    if "corp_name_list" in dart_df.columns:
        dart_df["corp_name"] = dart_df["corp_name_list"].fillna(dart_df.get("corp_name_map"))
    else:
        dart_df["corp_name"] = dart_df.get("corp_name_map")

    if "stock_code_list" in dart_df.columns:
        dart_df["stock_code"] = dart_df["stock_code_list"].fillna(dart_df.get("stock_code_map"))
    else:
        dart_df["stock_code"] = dart_df.get("stock_code_map")

    # 불필요한 중복 컬럼 제거
    drop_cols = [c for c in ["corp_name_list","corp_name_map","stock_code_list","stock_code_map"] if c in dart_df.columns]
    dart_df = dart_df.drop(columns=drop_cols)

    # 5) 보기 좋은 컬럼 순서로 저장
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

    # quick sanity
    print("[check] columns:", dart_df.columns.tolist())
    print("[check] head:")
    print(dart_df.head(3))

if __name__ == "__main__":
    main()
