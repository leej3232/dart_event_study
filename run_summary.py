import os
import pandas as pd

def car_for_window(g: pd.DataFrame, a: int, b: int) -> float:
    """
    CAR(a,b) = sum_{tau=a..b} ret_close
    - tau=0의 ret_close는 NaN(첫 행)이라 합에서 자동 제외되지만,
      보통 이벤트스터디에서는 tau=0 수익률도 포함시키고 싶으면
      Close 기반 수익률을 다시 정의해야 함.
    여기서는 'ret_close' 정의를 그대로 사용하고,
    tau=0의 NaN은 0으로 처리해서 포함되게 만든다.
    """
    w = g[(g["tau"] >= a) & (g["tau"] <= b)].copy()
    rc = w["ret_close"].fillna(0.0)
    return float(rc.sum())

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

    # --- event-level summary ---
    meta_cols = ["rcept_no","rcept_dt","event_date","event_trade_date","stock_code","corp_code","corp_name","report_nm","pblntf_ty","pblntf_detail_ty"]
    meta_cols = [c for c in meta_cols if c in df.columns]

    # event별로 CAR 계산
    summary_rows = []
    for rcept_no, g in df.groupby("rcept_no", sort=False):
        g = g.sort_values("tau")
        meta = g.iloc[0][meta_cols].to_dict()

        meta["CAR_m1_p1"] = car_for_window(g, -1, 1)
        meta["CAR_m5_p5"] = car_for_window(g, -5, 5)
        meta["CAR_m20_p20"] = car_for_window(g, -20, 20)

        # 이벤트 당일(=tau=0) 정규화 종가 확인용
        if "close_norm" in g.columns:
            meta["close_norm_tau0"] = float(g.loc[g["tau"] == 0, "close_norm"].iloc[0])
        summary_rows.append(meta)

    event_summary = pd.DataFrame(summary_rows)

    os.makedirs("data/processed", exist_ok=True)
    out1 = "data/processed/event_summary.csv"
    event_summary.to_csv(out1, index=False)
    print(f"[done] saved {out1} rows={len(event_summary)} cols={len(event_summary.columns)}")

    # --- AAR/CAAR (전체 평균) ---
    # tau별 평균 수익률(AAR)과 누적(CAAR)
    tmp = df.copy()
    tmp["ret_close_filled"] = tmp["ret_close"].fillna(0.0)

    caar = (
        tmp.groupby("tau", as_index=False)
           .agg(AAR=("ret_close_filled","mean"), N=("ret_close_filled","size"))
           .sort_values("tau")
           .reset_index(drop=True)
    )
    caar["CAAR"] = caar["AAR"].cumsum()

    out2 = "data/processed/caar.csv"
    caar.to_csv(out2, index=False)
    print(f"[done] saved {out2} rows={len(caar)} cols={len(caar.columns)}")

    print("[check] event_summary head:")
    print(event_summary.head(3)[["rcept_no","stock_code","CAR_m1_p1","CAR_m5_p5","CAR_m20_p20"]])

    print("[check] caar head:")
    print(caar.head(3))

if __name__ == "__main__":
    main()
