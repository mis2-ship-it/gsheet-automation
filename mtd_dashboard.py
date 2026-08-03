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

print("=" * 60)
print("DATE RANGE CHECK")
print("=" * 60)

print("Min Date :", final_df["Date"].min())
print("Max Date :", final_df["Date"].max())

print()

print(final_df["Date"].value_counts().sort_index())

# =========================================================
# DATE PREPARATION
# =========================================================

from datetime import datetime, timedelta

final_df["Date"] = pd.to_datetime(
    final_df["Date"]
)

# =========================================================
# BUSINESS DATE
# =========================================================

now = datetime.now()

# Business day starts at 09:00 AM
if now.hour < 9:
    business_day = (now - timedelta(days=1)).date()
else:
    business_day = now.date()

today = pd.Timestamp(business_day)

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

# =========================================================
# FILTER DATA
# =========================================================

today_df = final_df[
    final_df["Date"] == today
].copy()

lw_df = final_df[
    final_df["Date"] == last_week
].copy()

l2w_df = final_df[
    final_df["Date"] == last_2_week
].copy()

mom_df = final_df[
    final_df["Date"] == last_month
].copy()

ly_df = final_df[
    final_df["Date"] == last_year
].copy()

print("=" * 60)
print("FILTER CHECK")
print("=" * 60)

print("Today Rows     :", len(today_df))
print("LW Rows        :", len(lw_df))
print("L2W Rows       :", len(l2w_df))
print("MoM Rows       :", len(mom_df))
print("LY Rows        :", len(ly_df))

# =========================================================
# KPI SUMMARY
# =========================================================

def get_kpi(df):

    if df.empty:

        return {
            "Gross": 0,
            "Net": 0,
            "Discount": 0,
            "Orders": 0,
            "Qty": 0,
            "AOV": 0,
            "Dis %": 0
        }

    gross = df["Gross Sales"].sum()
    net = df["Net Sales"].sum()
    discount = df["Discount"].sum()
    orders = df["Orders"].sum()
    qty = df["Quantity"].sum()

    aov = net / orders if orders else 0
    dis_pct = (discount / gross * 100) if gross else 0

    return {
        "Gross": round(gross, 2),
        "Net": round(net, 2),
        "Discount": round(discount, 2),
        "Orders": int(orders),
        "Qty": round(qty, 2),
        "AOV": round(aov, 2),
        "Dis %": round(dis_pct, 2)
    }


today_kpi = get_kpi(today_df)
lw_kpi = get_kpi(lw_df)
l2w_kpi = get_kpi(l2w_df)
mom_kpi = get_kpi(mom_df)
ly_kpi = get_kpi(ly_df)

print("=" * 60)
print("TODAY KPI")
print("=" * 60)

for key, value in today_kpi.items():
    print(f"{key:12} : {value}")
