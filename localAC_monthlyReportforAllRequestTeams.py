import os
import pandas as pd
from datetime import datetime, timedelta

# ================================================
# 📁 USER INPUT — PROVIDE MONTH FOLDER
# ================================================
base_folder = r"C:\Hieu Nguyen\AC Local Raw"  # parent folder
month_folder = input("📂 Enter the month folder (e.g. 2025-09): ").strip()

folder_path = os.path.join(base_folder, month_folder)
if not os.path.exists(folder_path):
    raise FileNotFoundError(f"❌ Folder not found: {folder_path}")

output_path = os.path.join(folder_path, f"aggregate_{month_folder}.csv")

# ================================================
# 🧩 FUNCTION — LOAD & COMBINE ALL CSV
# ================================================
def load_all_csvs(folder):
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files:
        raise FileNotFoundError("❌ No CSV files found in this folder.")
    print(f"🔍 Found {len(files)} CSV file(s).")
    return pd.concat([pd.read_csv(os.path.join(folder, f)) for f in files], ignore_index=True)

# ================================================
# 🧠 FUNCTION — CLEAN & AGGREGATE
# ================================================
def compile_adjustment_data(df):
    # --- Clean amount
    df["amount"] = df["amount"].astype(str).str.replace(",", "", regex=False)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # --- Parse datetime
    df["ac_create_time"] = pd.to_datetime(df["ac_create_time"], errors="coerce")
    df = df.dropna(subset=["ac_create_time"])

    # --- Derive date anchors
    df["ac_create_date"] = df["ac_create_time"].dt.strftime("%Y-%m-%d")
    df["ac_create_month"] = df["ac_create_time"].dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-%d")  # first day of month

    # get Monday of that week
    df["ac_create_week"] = (df["ac_create_time"] - pd.to_timedelta(df["ac_create_time"].dt.dayofweek, unit="d"))
    df["ac_create_week"] = df["ac_create_week"].dt.strftime("%Y-%m-%d")

    # --- Normalize amount sign by dashboard
    dash = df["dashboard"].astype(str).str.lower().str.strip()
    df.loc[dash.str.contains("inbound", na=False), "amount"] = -df["amount"].abs()
    df.loc[dash.str.contains("outbound", na=False), "amount"] = df["amount"].abs()

    # --- Aggregate
    agg = (
        df.groupby(
            ["dashboard", "ac_create_month", "ac_create_week", "ac_create_date",
             "requester", "adjustment_status"],
            dropna=False
        )
        .agg(
            adjustment_count=("adjustment_id", "nunique"),
            total_amount=("amount", "sum")
        )
        .reset_index()
    )

    return agg

# ================================================
# 🚀 MAIN EXECUTION
# ================================================
if __name__ == "__main__":
    print(f"🌀 Loading CSVs from: {folder_path}")
    df = load_all_csvs(folder_path)

    print("⚙️ Compiling data...")
    result = compile_adjustment_data(df)

    print("💾 Saving output...")
    result.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ Done! Saved to: {output_path}")
