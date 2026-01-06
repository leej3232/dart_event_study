import os
import pandas as pd

def pick_return_column(df: pd.DataFrame) -> str:
    # FinanceDataReader가 주는 Change(일별 수익률)가 있으면 그걸 쓰는 게 가장 깔끔
    if "Change" in df.columns:
        return "Change"
    # fallback
    if "ret_close" in df.columns:
        return "ret_close"
    raise RuntimeError("No return column found. Need either 'Change' or 'ret_close' in event_panel.csv")

def car_for_window(g: pd.DataFrame, ret_col: str, a: int, b: int) -> float:
    w = g[(g["tau"] >= a) & (g["tau"] <= b)]
    return float(w[ret_col].dropna().sum())

def main():
    panel_path = "data/processed/event_panel.csv"
    if not os.path.exists(panel_path):
        raise RuntimeError("event_panel.csv not found. Run: python run_panel.py")

    df = pd.read_csv(
        panel_path,
        dtype={"rcept_no":"string","stock_code":"string","corp_code":"string"},
        parse_dates=["Date","event_date","event_trade_date"],
    )
    df["stock_code"] = df["stock_code"].astype("string").str.zfill(6)

    ret_col = pick_return_column(df)
    # 이름 통일: AR(Abnormal Return처럼 쓰고 싶으면 시장조정에서 진짜 AR이 됨)
    df["AR"] = pd.to_numeric(df[ret_col], errors="coerce")

    # --- event-level summary ---
    meta_cols = [
        "rcept_no","rcept_dt","event_date","event_trade_date",
        "stock_code","corp_code","corp_name","report_nm",
        "pblntf_ty","pblntf_detail_ty"
    ]
    meta_cols = [c for c in meta_cols if c in df.columns]

    summary_rows = []
    for rcept_no, g in df.groupby("rcept_no", sort=False):
        g = g.sort_values("tau")
        meta = g.iloc[0][meta_cols].to_dict()

        meta["ret_source"] = ret_col
        meta["CAR_m1_p1"]   = car_for_window(g, "AR", -1, 1)
        meta["CAR_m5_p5"]   = car_for_window(g, "AR", -5, 5)
        meta["CAR_m20_p20"] = car_for_window(g, "AR", -20, 20)

        if "close_norm" in g.columns:
            meta["close_norm_tau0"] = float(g.loc[g["tau"] == 0, "close_norm"].iloc[0])

        summary_rows.append(meta)

    event_summary = pd.DataFrame(summary_rows)

    os.makedirs("data/processed", exist_ok=True)
    out1 = "data/processed/event_summary.csv"
    event_summary.to_csv(out1, index=False)
    print(f"[done] saved {out1} rows={len(event_summary)} cols={len(event_summary.columns)}")

    # --- AAR/CAAR ---
    caar = (
        df.groupby("tau", as_index=False)
          .agg(AAR=("AR","mean"), N=("AR","count"))
          .sort_values("tau")
          .reset_index(drop=True)
    )
    caar["CAAR"] = caar["AAR"].cumsum()

    out2 = "data/processed/caar.csv"
    caar.to_csv(out2, index=False)
    print(f"[done] saved {out2} rows={len(caar)} cols={len(caar.columns)}")

    print("[check] return source:", ret_col)
    print("[check] event_summary head:")
    print(event_summary.head(3)[["rcept_no","stock_code","CAR_m1_p1","CAR_m5_p5","CAR_m20_p20"]])

    print("[check] caar head:")
    print(caar.head(3))

if __name__ == "__main__":
    main()
