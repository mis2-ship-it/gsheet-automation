# =========================================================
# IMPORTS
# =========================================================

from pathlib import Path
import pandas as pd

print("=" * 60)
print("🚀 MTD DASHBOARD STARTED")
print("=" * 60)

# =========================================================
# FIND ALL MONTHLY CSV FILES
# =========================================================

BASE_FOLDER = Path("monthly_data")

csv_files = sorted(
    BASE_FOLDER.rglob("*.csv")
)

print(f"📂 CSV Files Found : {len(csv_files)}")

if len(csv_files) == 0:
    raise Exception("❌ No Monthly CSV Files Found")

# =========================================================
# LATEST MONTH FILE
# =========================================================

latest_csv = csv_files[-1]

print(f"📄 Latest File : {latest_csv}")

# =========================================================
# READ CSV
# =========================================================

final_df = pd.read_csv(
    latest_csv,
    low_memory=False
)

print("=" * 60)
print("TOTAL ROWS :", len(final_df))
print("TOTAL COLUMNS :", len(final_df.columns))
print("=" * 60)

print(final_df.head())

print("=" * 60)
print("COLUMN LIST")
print("=" * 60)

print(final_df.columns.tolist())

# =========================================================
# DATE PREPARATION
# =========================================================

from datetime import datetime, timedelta

final_df["Date"] = pd.to_datetime(
    final_df["Date"]
)

today = final_df["Date"].max()

last_week = today - timedelta(days=7)

last_2_week = today - timedelta(days=14)

last_month = today - pd.DateOffset(months=1)

last_year = today - pd.DateOffset(years=1)

print("=" * 60)
print("DATE CHECK")
print("=" * 60)

print("Today      :", today.date())
print("Last Week  :", last_week.date())
print("Last 2 Week:", last_2_week.date())
print("Last Month :", last_month.date())
print("Last Year  :", last_year.date())
