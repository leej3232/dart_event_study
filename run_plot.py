import os
import pandas as pd
import matplotlib.pyplot as plt

def main():
    caar_path = "data/processed/caar.csv"
    if not os.path.exists(caar_path):
        raise RuntimeError("caar.csv not found. Run: python run_summary.py")

    df = pd.read_csv(caar_path)
    os.makedirs("reports/figures", exist_ok=True)

    # CAAR plot
    plt.figure()
    plt.plot(df["tau"], df["CAAR"])
    plt.axvline(0)
    plt.xlabel("tau (trading days)")
    plt.ylabel("CAAR")
    plt.title("CAAR around filing date (tau=0)")
    out1 = "reports/figures/caar.png"
    plt.savefig(out1, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"[done] saved {out1}")

    # AAR plot
    plt.figure()
    plt.plot(df["tau"], df["AAR"])
    plt.axvline(0)
    plt.xlabel("tau (trading days)")
    plt.ylabel("AAR")
    plt.title("AAR around filing date (tau=0)")
    out2 = "reports/figures/aar.png"
    plt.savefig(out2, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"[done] saved {out2}")

if __name__ == "__main__":
    main()
