# =========================================================
# mtd_dashboard.py  (corrected: growth, totals, display formatting)
# =========================================================

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import json
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ------------------------------
# Config
# ------------------------------
EMAIL = os.environ.get("EMAIL_USER")
PASSWORD = os.environ.get("EMAIL_PASS")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
BASE_FOLDER = Path("monthly_data")

# ------------------------------
# Helpers
# ------------------------------
def find_latest_csv(base_folder: Path):
    csv_files = list(base_folder.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under {base_folder}")
    month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    def extract_month_year(path: Path):
        m = re.search(r"MTD_(\w{3})_(\d{2})\.csv", path.name)
        if not m:
            return datetime(1900,1,1)
        month = month_map.get(m.group(1), 1)
        year = 2000 + int(m.group(2))
        return datetime(year, month, 1)
    latest = max(csv_files, key=extract_month_year)
    return latest

def to_date_safe(x):
    if x is None:
        return None
    return pd.to_datetime(x).date()

def normalize_region(x):
    if pd.isna(x): return "Others"
    s = str(x).strip().upper()
    if s.startswith("KA"): return "KA"
    if s.startswith("MH"): return "MH"
    if s in ("TN", "TAMIL NADU"): return "TN"
    if s.startswith("KER") or s == "KERALA": return "KL"
    return s

def normalize_source(x):
    if pd.isna(x): return "Others"
    s = str(x).strip().lower()
    if "swiggy" in s: return "Swiggy"
    if "zomato" in s: return "Zomato"
    if "own" in s or "ownly" in s: return "Ownly"
    if "store" in s or "in store" in s or "instore" in s: return "In Store"
    return "Others"

def format_money(x):
    try:
        return f"₹{int(round(x)):,}"
    except Exception:
        return "₹0"

def format_pct(x):
    if pd.isna(x):
        return "-"
    try:
        return f"{x:.1f}%"
    except Exception:
        return "-"

# ------------------------------
# Load CSV and basic filtering
# ------------------------------
latest_csv = find_latest_csv(BASE_FOLDER)
final_df = pd.read_csv(latest_csv, low_memory=False)

if "Date" not in final_df.columns:
    raise KeyError("Date column not found in CSV")

final_df["Date"] = pd.to_datetime(final_df["Date"], errors="coerce")
final_df["Store Type"] = final_df.get("Store Type", "").fillna("").astype(str).str.strip().str.upper()

for col in ["Net Sales","Gross Sales","Discount","Orders"]:
    if col not in final_df.columns:
        final_df[col] = 0

available_dates = sorted(final_df["Date"].dropna().unique())
if not available_dates:
    raise ValueError("No valid dates in data")

ftd_date = available_dates[-1]
mtd_start = pd.Timestamp(year=ftd_date.year, month=ftd_date.month, day=1)

# nearest-available date helper
def nearest_available_timestamp(target_ts, available_list):
    if target_ts is None:
        return None
    valid = [d for d in available_list if d <= target_ts]
    return valid[-1] if valid else None

# compute target timestamps and nearest available
target_lw_ts = ftd_date - timedelta(days=7)
target_l2w_ts = ftd_date - timedelta(days=14)
target_mom_ts = ftd_date - pd.DateOffset(months=1)
target_ly_ts = ftd_date - pd.DateOffset(years=1)

last_week_ts = nearest_available_timestamp(target_lw_ts, available_dates)
last_2_week_ts = nearest_available_timestamp(target_l2w_ts, available_dates)
last_month_ts = nearest_available_timestamp(target_mom_ts, available_dates)
last_year_ts = nearest_available_timestamp(target_ly_ts, available_dates)

last_week = to_date_safe(last_week_ts)
last_2_week = to_date_safe(last_2_week_ts)
last_month = to_date_safe(last_month_ts)
last_year = to_date_safe(last_year_ts)
ftd_date_only = to_date_safe(ftd_date)

def df_on_date(df, date_obj):
    if date_obj is None:
        return df.iloc[0:0].copy()
    return df.loc[df["Date"].dt.date == date_obj].copy()

ftd_df = df_on_date(final_df, ftd_date_only)
lw_df = df_on_date(final_df, last_week)
l2w_df = df_on_date(final_df, last_2_week)
mom_df = df_on_date(final_df, last_month)
ly_df = df_on_date(final_df, last_year)
mtd_df = final_df.loc[(final_df["Date"] >= mtd_start) & (final_df["Date"] <= ftd_date)].copy()

ftd_coco_df = ftd_df[ftd_df["Store Type"] == "COCO"].copy()
lw_coco_df = lw_df[lw_df["Store Type"] == "COCO"].copy()
l2w_coco_df = l2w_df[l2w_df["Store Type"] == "COCO"].copy()
mtd_coco_df = mtd_df[mtd_df["Store Type"] == "COCO"].copy()

# ------------------------------
# KPI (COCO only) — Qty removed
# ------------------------------
def get_kpi(df):
    gross = df["Gross Sales"].sum()
    net = df["Net Sales"].sum()
    discount = df["Discount"].sum()
    orders = df["Orders"].sum()
    aov = net / orders if orders else 0
    dis_pct = (discount / gross * 100) if gross else 0
    return {"Gross": round(gross,2), "Net": round(net,2), "Discount": round(discount,2), "Orders": int(orders), "AOV": round(aov,2), "Dis %": round(dis_pct,2)}

ftd_kpi = get_kpi(ftd_coco_df)
mtd_kpi = get_kpi(mtd_coco_df)

# ------------------------------
# Build comparisons (Brand / Region / Source)
# ------------------------------
import numpy as np

def build_group_compare_numeric(ftd_series, lw_series, l2w_series, label):
    idx = sorted(set(ftd_series.index).union(lw_series.index).union(l2w_series.index))
    df = pd.DataFrame(index=idx)
    df.index.name = label
    df["FTD"] = ftd_series.reindex(idx).fillna(0).astype(float)
    df["LW"]  = lw_series.reindex(idx).fillna(0).astype(float)
    df["L2W"] = l2w_series.reindex(idx).fillna(0).astype(float)
    # compute Growth % as NaN when LW == 0
    df["Growth %"] = np.where(df["LW"] > 0, ((df["FTD"] - df["LW"]) / df["LW"]) * 100, pd.NA)
    return df.reset_index()

def build_dis_pct_numeric(ftd_df, lw_df, l2w_df, group_col):
    f = ftd_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    l = lw_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    l2 = l2w_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    f["Dis%"] = np.where(f["Gross"]>0, f["Discount"]/f["Gross"]*100, 0)
    l["Dis%"] = np.where(l["Gross"]>0, l["Discount"]/l["Gross"]*100, 0)
    l2["Dis%"] = np.where(l2["Gross"]>0, l2["Discount"]/l2["Gross"]*100, 0)
    df = pd.merge(f[[group_col,"Dis%"]], l[[group_col,"Dis%"]], on=group_col, how="outer", suffixes=("_FTD","_LW")).fillna(0)
    df = pd.merge(df, l2[[group_col,"Dis%"]], on=group_col, how="left").fillna(0)
    df = df.rename(columns={group_col:"Group", "Dis%":"Dis%_L2W"})
    df["Change"] = df["Dis%_FTD"] - df["Dis%_LW"]
    return df

# Brand
ftd_brand_net = ftd_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
lw_brand_net = lw_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
l2w_brand_net = l2w_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
brand_net_num = build_group_compare_numeric(ftd_brand_net, lw_brand_net, l2w_brand_net, "Brand")
brand_dis_num = build_dis_pct_numeric(ftd_coco_df, lw_coco_df, l2w_coco_df, "Brand Name")

brand_num = pd.merge(brand_net_num, brand_dis_num, left_on="Brand", right_on="Group", how="left").drop(columns=["Group"]).fillna(0)
# rename cols
brand_num = brand_num.rename(columns={"Brand":"Brand Name", "Dis%_FTD":"Dis%_FTD", "Dis%_LW":"Dis%_LW", "Dis%_L2W":"Dis%_L2W", "Change":"Dis_Change"})
# total row
total_net_ftd = brand_num["FTD"].sum()
total_net_lw  = brand_num["LW"].sum()
total_net_l2w = brand_num["L2W"].sum()
total_discount_ftd = ftd_coco_df["Discount"].sum(); total_gross_ftd = ftd_coco_df["Gross Sales"].sum()
total_discount_lw = lw_coco_df["Discount"].sum(); total_gross_lw = lw_coco_df["Gross Sales"].sum()
total_discount_l2w = l2w_coco_df["Discount"].sum(); total_gross_l2w = l2w_coco_df["Gross Sales"].sum()
total_dis_ftd_pct = (total_discount_ftd / total_gross_ftd * 100) if total_gross_ftd else 0
total_dis_lw_pct = (total_discount_lw / total_gross_lw * 100) if total_gross_lw else 0
total_dis_l2w_pct = (total_discount_l2w / total_gross_l2w * 100) if total_gross_l2w else 0
total_growth_pct = ((total_net_ftd - total_net_lw) / total_net_lw * 100) if total_net_lw>0 else pd.NA

# prepare brand_display_with_total (numeric -> then format)
brand_display_numeric = brand_num[["Brand Name","FTD","LW","Growth %","L2W","Dis%_FTD","Dis%_LW","Dis_Change","Dis%_L2W"]].copy()
brand_display_numeric = brand_display_numeric.rename(columns={"Dis%_FTD":"Dis FTD","Dis%_LW":"Dis LW","Dis_Change":"Dis Change","Dis%_L2W":"Dis L2W"})
total_row_brand = {
    "Brand Name":"Total",
    "FTD": total_net_ftd,
    "LW": total_net_lw,
    "Growth %": total_growth_pct,
    "L2W": total_net_l2w,
    "Dis FTD": total_dis_ftd_pct,
    "Dis LW": total_dis_lw_pct,
    "Dis Change": total_dis_ftd_pct - total_dis_lw_pct,
    "Dis L2W": total_dis_l2w_pct
}
brand_display_with_total = pd.concat([brand_display_numeric, pd.DataFrame([total_row_brand])], ignore_index=True).fillna(0)

# Format brand_display_with_total
brand_display_with_total["FTD"] = brand_display_with_total["FTD"].apply(format_money)
brand_display_with_total["LW"]  = brand_display_with_total["LW"].apply(format_money)
brand_display_with_total["L2W"] = brand_display_with_total["L2W"].apply(format_money)
brand_display_with_total["Growth %"] = brand_display_with_total["Growth %"].apply(format_pct)
brand_display_with_total["Dis FTD"] = brand_display_with_total["Dis FTD"].apply(format_pct)
brand_display_with_total["Dis LW"] = brand_display_with_total["Dis LW"].apply(format_pct)
brand_display_with_total["Dis Change"] = brand_display_with_total["Dis Change"].apply(format_pct)
brand_display_with_total["Dis L2W"] = brand_display_with_total["Dis L2W"].apply(format_pct)

# Region
ftd_region_net = ftd_coco_df.assign(RegionNorm=ftd_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
lw_region_net = lw_coco_df.assign(RegionNorm=lw_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
l2w_region_net = l2w_coco_df.assign(RegionNorm=l2w_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
region_net_num = build_group_compare_numeric(ftd_region_net, lw_region_net, l2w_region_net, "Region")
region_dis_num = build_dis_pct_numeric(
    ftd_coco_df.assign(RegionNorm=ftd_coco_df["Region"].apply(normalize_region)),
    lw_coco_df.assign(RegionNorm=lw_coco_df["Region"].apply(normalize_region)),
    l2w_coco_df.assign(RegionNorm=l2w_coco_df["Region"].apply(normalize_region)),
    "RegionNorm"
)
region_num = pd.merge(region_net_num, region_dis_num, left_on="Region", right_on="Group", how="left").drop(columns=["Group"]).fillna(0)
region_num = region_num.rename(columns={"Region":"Region Name","Dis%_FTD":"Dis%_FTD","Dis%_LW":"Dis%_LW","Dis%_L2W":"Dis%_L2W","Change":"Dis_Change"})

# totals for region
total_net_ftd_r = region_num["FTD"].sum(); total_net_lw_r = region_num["LW"].sum(); total_net_l2w_r = region_num["L2W"].sum()
total_dis_ftd_r = ftd_coco_df["Discount"].sum(); total_gross_ftd_r = ftd_coco_df["Gross Sales"].sum()
total_dis_lw_r = lw_coco_df["Discount"].sum(); total_gross_lw_r = lw_coco_df["Gross Sales"].sum()
total_dis_l2w_r = l2w_coco_df["Discount"].sum(); total_gross_l2w_r = l2w_coco_df["Gross Sales"].sum()
total_dis_ftd_pct_r = (total_dis_ftd_r/total_gross_ftd_r*100) if total_gross_ftd_r else 0
total_dis_lw_pct_r = (total_dis_lw_r/total_gross_lw_r*100) if total_gross_lw_r else 0
total_dis_l2w_pct_r = (total_dis_l2w_r/total_gross_l2w_r*100) if total_gross_l2w_r else 0
total_growth_pct_r = ((total_net_ftd_r - total_net_lw_r)/total_net_lw_r*100) if total_net_lw_r>0 else pd.NA

region_display_numeric = region_num.rename(columns={"Region Name":"Region"})[["Region","FTD","LW","Growth %","L2W","Dis%_FTD","Dis%_LW","Dis_Change","Dis%_L2W"]].copy()
region_display_numeric = region_display_numeric.rename(columns={"Dis%_FTD":"Dis FTD","Dis%_LW":"Dis LW","Dis_Change":"Dis Change","Dis%_L2W":"Dis L2W"})
total_row_region = {"Region":"Total","FTD":total_net_ftd_r,"LW":total_net_lw_r,"Growth %":total_growth_pct_r,"L2W":total_net_l2w_r,"Dis FTD":total_dis_ftd_pct_r,"Dis LW":total_dis_lw_pct_r,"Dis Change":total_dis_ftd_pct_r-total_dis_lw_pct_r,"Dis L2W":total_dis_l2w_pct_r}
region_display_with_total = pd.concat([region_display_numeric, pd.DataFrame([total_row_region])], ignore_index=True).fillna(0)
region_display_with_total["FTD"] = region_display_with_total["FTD"].apply(format_money)
region_display_with_total["LW"] = region_display_with_total["LW"].apply(format_money)
region_display_with_total["L2W"] = region_display_with_total["L2W"].apply(format_money)
region_display_with_total["Growth %"] = region_display_with_total["Growth %"].apply(format_pct)
region_display_with_total["Dis FTD"] = region_display_with_total["Dis FTD"].apply(format_pct)
region_display_with_total["Dis LW"] = region_display_with_total["Dis LW"].apply(format_pct)
region_display_with_total["Dis Change"] = region_display_with_total["Dis Change"].apply(format_pct)
region_display_with_total["Dis L2W"] = region_display_with_total["Dis L2W"].apply(format_pct)

# Source
ftd_source_net = ftd_coco_df.assign(SourceNorm=ftd_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
lw_source_net = lw_coco_df.assign(SourceNorm=lw_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
l2w_source_net = l2w_coco_df.assign(SourceNorm=l2w_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
source_net_num = build_group_compare_numeric(ftd_source_net, lw_source_net, l2w_source_net, "Source")
source_dis_num = build_dis_pct_numeric(
    ftd_coco_df.assign(SourceNorm=ftd_coco_df["Source"].apply(normalize_source)),
    lw_coco_df.assign(SourceNorm=lw_coco_df["Source"].apply(normalize_source)),
    l2w_coco_df.assign(SourceNorm=l2w_coco_df["Source"].apply(normalize_source)),
    "SourceNorm"
)
source_num = pd.merge(source_net_num, source_dis_num, left_on="Source", right_on="Group", how="left").drop(columns=["Group"]).fillna(0)
source_num = source_num.rename(columns={"Source":"Source Name","Dis%_FTD":"Dis%_FTD","Dis%_LW":"Dis%_LW","Dis%_L2W":"Dis%_L2W","Change":"Dis_Change"})

# totals for source
total_net_ftd_s = source_num["FTD"].sum(); total_net_lw_s = source_num["LW"].sum(); total_net_l2w_s = source_num["L2W"].sum()
total_dis_ftd_s = ftd_coco_df["Discount"].sum(); total_gross_ftd_s = ftd_coco_df["Gross Sales"].sum()
total_dis_lw_s = lw_coco_df["Discount"].sum(); total_gross_lw_s = lw_coco_df["Gross Sales"].sum()
total_dis_l2w_s = l2w_coco_df["Discount"].sum(); total_gross_l2w_s = l2w_coco_df["Gross Sales"].sum()
total_dis_ftd_pct_s = (total_dis_ftd_s/total_gross_ftd_s*100) if total_gross_ftd_s else 0
total_dis_lw_pct_s = (total_dis_lw_s/total_gross_lw_s*100) if total_gross_lw_s else 0
total_dis_l2w_pct_s = (total_dis_l2w_s/total_gross_l2w_s*100) if total_gross_l2w_s else 0
total_growth_pct_s = ((total_net_ftd_s - total_net_lw_s)/total_net_lw_s*100) if total_net_lw_s>0 else pd.NA

source_display_numeric = source_num.rename(columns={"Source Name":"Source"})[["Source","FTD","LW","Growth %","L2W","Dis%_FTD","Dis%_LW","Dis_Change","Dis%_L2W"]].copy()
source_display_numeric = source_display_numeric.rename(columns={"Dis%_FTD":"Dis FTD","Dis%_LW":"Dis LW","Dis_Change":"Dis Change","Dis%_L2W":"Dis L2W"})
total_row_source = {"Source":"Total","FTD":total_net_ftd_s,"LW":total_net_lw_s,"Growth %":total_growth_pct_s,"L2W":total_net_l2w_s,"Dis FTD":total_dis_ftd_pct_s,"Dis LW":total_dis_lw_pct_s,"Dis Change":total_dis_ftd_pct_s-total_dis_lw_pct_s,"Dis L2W":total_dis_l2w_pct_s}
source_display_with_total = pd.concat([source_display_numeric, pd.DataFrame([total_row_source])], ignore_index=True).fillna(0)
source_display_with_total["FTD"] = source_display_with_total["FTD"].apply(format_money)
source_display_with_total["LW"] = source_display_with_total["LW"].apply(format_money)
source_display_with_total["L2W"] = source_display_with_total["L2W"].apply(format_money)
source_display_with_total["Growth %"] = source_display_with_total["Growth %"].apply(format_pct)
source_display_with_total["Dis FTD"] = source_display_with_total["Dis FTD"].apply(format_pct)
source_display_with_total["Dis LW"] = source_display_with_total["Dis LW"].apply(format_pct)
source_display_with_total["Dis Change"] = source_display_with_total["Dis Change"].apply(format_pct)
source_display_with_total["Dis L2W"] = source_display_with_total["Dis L2W"].apply(format_pct)

# ------------------------------
# KPI table top (COCO only)
# ------------------------------
kpi_table = pd.DataFrame([{
    "Metric":"Gross","FTD":format_money(ftd_kpi["Gross"]), "MTD":format_money(mtd_kpi["Gross"])
},{
    "Metric":"Net","FTD":format_money(ftd_kpi["Net"]), "MTD":format_money(mtd_kpi["Net"])
},{
    "Metric":"Discount","FTD":format_money(ftd_kpi["Discount"]), "MTD":format_money(mtd_kpi["Discount"])
},{
    "Metric":"Orders","FTD":f"{ftd_kpi['Orders']:,}", "MTD":f"{mtd_kpi['Orders']:,}"
},{
    "Metric":"AOV","FTD":format_money(ftd_kpi["AOV"]), "MTD":format_money(mtd_kpi["AOV"])
},{
    "Metric":"Discount %","FTD":format_pct(ftd_kpi["Dis %"]), "MTD":format_pct(mtd_kpi["Dis %"])
}])

# ------------------------------
# Build Email HTML (using *_with_total frames)
# ------------------------------
html_body = f"""
<html>
<head>
  <style>
    body{{font-family:Calibri, Arial, sans-serif; font-size:13px; background:#fff; color:#222}}
    h2{{background:#243447;color:#fff;padding:8px}}
    h3{{background:#f2f2f2;padding:6px}}
    table.table{{border-collapse:collapse; width:95%; margin-bottom:18px}}
    table.table th{{background:#243447;color:#fff;padding:6px; text-align:center}}
    table.table td{{padding:6px; border:1px solid #ddd; text-align:center}}
  </style>
</head>
<body>
<h2>📊 Brand Dashboard (COCO) — FTD vs LW (FTD: {ftd_date_only})</h2>

<div>
  <h3>KPIs (COCO only)</h3>
  {kpi_table.to_html(index=False, border=0, classes="table")}
</div>

<div>
  <h3>Brand Dashboard (COCO) — FTD vs LW</h3>
  {brand_display_with_total.to_html(index=False, border=0, classes="table")}
</div>

<div>
  <h3>Region Dashboard (COCO) — FTD vs LW</h3>
  {region_display_with_total.to_html(index=False, border=0, classes="table")}
</div>

<div>
  <h3>Source Dashboard (COCO) — FTD vs LW</h3>
  {source_display_with_total.to_html(index=False, border=0, classes="table")}
</div>

</body>
</html>
"""

# ------------------------------
# Send email
# ------------------------------
def send_mail(subject, body, to_addrs):
    msg = MIMEMultipart()
    msg["From"] = EMAIL
    msg["To"] = ",".join(to_addrs)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    s = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    s.starttls()
    s.login(EMAIL, PASSWORD)
    s.sendmail(EMAIL, to_addrs, msg.as_string())
    s.quit()
    print("✅ Mail sent to:", to_addrs)

TO = [os.environ.get("RECIPIENTS_TO", "mis2@frozenbottle.in")]
send_mail(subject=f"COCO Dashboard | {ftd_date_only}", body=html_body, to_addrs=TO)
