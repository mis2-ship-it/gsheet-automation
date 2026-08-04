# name: mtd_dashboard.py
# Send COCO-only dashboard by email. No Google Sheets pushes.

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import json
import os
import re
import math
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

# Where monthly CSVs live
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

def pivot_to_html(df):
    # Small helper to produce compact HTML for DataFrame
    return (
        df.round(2)
          .to_html(index=True, border=0, classes="table")
    )

def pct(v):
    return f"{v:.1f}%" if not pd.isna(v) else ""

# Normalizers
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

# ------------------------------
# Load CSV
# ------------------------------
print("📂 Finding latest CSV...")
latest_csv = find_latest_csv(BASE_FOLDER)
print("📄 Using:", latest_csv)
final_df = pd.read_csv(latest_csv, low_memory=False)

# Defensive: ensure Date column exists
if "Date" not in final_df.columns:
    raise KeyError("Date column not found in CSV")

final_df["Date"] = pd.to_datetime(final_df["Date"], errors="coerce")
# Standardize Store Type for comparisons
if "Store Type" in final_df.columns:
    final_df["Store Type"] = final_df["Store Type"].fillna("").astype(str).str.strip().str.upper()
else:
    final_df["Store Type"] = ""

# Provide safe numeric columns
for col in ["Net Sales","Gross Sales","Discount","Orders"]:
    if col not in final_df.columns:
        final_df[col] = 0

# Available dates
available_dates = sorted(final_df["Date"].dropna().unique())
if not available_dates:
    raise ValueError("No valid dates in data")

ftd_date = available_dates[-1]  # latest available date/time
mtd_start = pd.Timestamp(year=ftd_date.year, month=ftd_date.month, day=1)

# compute same-day last week / last 2 week / last month / last year by date
last_week = to_date_safe(ftd_date - timedelta(days=7))
last_2_week = to_date_safe(ftd_date - timedelta(days=14))
last_month = to_date_safe(ftd_date - pd.DateOffset(months=1))
last_year = to_date_safe(ftd_date - pd.DateOffset(years=1))
ftd_date_only = to_date_safe(ftd_date)

# Filter datasets by date (date-only comparison)
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

print("FTD date:", ftd_date_only, "rows:", len(ftd_df))
print("LW date:", last_week, "rows:", len(lw_df))
print("L2W date:", last_2_week, "rows:", len(l2w_df))
print("MTD rows:", len(mtd_df))

# Focus only on COCO for KPIs and dashboards
ftd_coco_df = ftd_df[ftd_df["Store Type"] == "COCO"].copy()
lw_coco_df = lw_df[lw_df["Store Type"] == "COCO"].copy()
l2w_coco_df = l2w_df[l2w_df["Store Type"] == "COCO"].copy()
mtd_coco_df = mtd_df[mtd_df["Store Type"] == "COCO"].copy()

# ------------------------------
# KPI (COCO only) - remove Quantity
# ------------------------------
def get_kpi(df):
    gross = df["Gross Sales"].sum() if "Gross Sales" in df.columns else 0
    net = df["Net Sales"].sum() if "Net Sales" in df.columns else 0
    discount = df["Discount"].sum() if "Discount" in df.columns else 0
    orders = df["Orders"].sum() if "Orders" in df.columns else 0
    aov = net / orders if orders else 0
    dis_pct = (discount / gross * 100) if gross else 0
    return {
        "Gross": round(gross,2),
        "Net": round(net,2),
        "Discount": round(discount,2),
        "Orders": int(orders),
        "AOV": round(aov,2),
        "Dis %": round(dis_pct,2)
    }

ftd_kpi = get_kpi(ftd_coco_df)
mtd_kpi = get_kpi(mtd_coco_df)

# ------------------------------
# COCO comparison functions
# ------------------------------
def build_group_compare(ftd_df_grouped, lw_df_grouped, l2w_df_grouped, group_label):
    # ftd_df_grouped etc are results of groupby sums with index as group values
    df = pd.DataFrame(index=sorted(set(ftd_df_grouped.index).union(lw_df_grouped.index).union(l2w_df_grouped.index)))
    df.index.name = group_label
    df["FTD"] = ftd_df_grouped.reindex(df.index).fillna(0)
    df["LW"]  = lw_df_grouped.reindex(df.index).fillna(0)
    df["L2W"] = l2w_df_grouped.reindex(df.index).fillna(0)
    df["Growth %"] = ((df["FTD"] - df["LW"]) / df["LW"].replace(0,1)) * 100
    # Format columns for readability later
    return df.reset_index()

def build_group_dis_pct(ftd_df, lw_df, l2w_df, group_col):
    # returns df indexed by group_col with Dis% metrics
    f = ftd_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    l = lw_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    l2 = l2w_df.groupby(group_col).agg(Discount=("Discount","sum"), Gross=("Gross Sales","sum")).reset_index()
    f["Dis%"] = (f["Discount"] / f["Gross"].replace(0,1)) * 100
    l["Dis%"] = (l["Discount"] / l["Gross"].replace(0,1)) * 100
    l2["Dis%"] = (l2["Discount"] / l2["Gross"].replace(0,1)) * 100
    # merge all three
    df = pd.merge(f[[group_col,"Dis%"]], l[[group_col,"Dis%"]], on=group_col, how="outer", suffixes=("_FTD","_LW")).fillna(0)
    df = pd.merge(df, l2[[group_col,"Dis%"]], on=group_col, how="left").fillna(0)
    df = df.rename(columns={"Dis%":"Dis%_L2W"})
    # ensure columns
    df["Change"] = df["Dis%_FTD"] - df["Dis%_LW"]
    # reorder
    df = df[[group_col, "Dis%_FTD", "Dis%_LW", "Change", "Dis%_L2W"]]
    df = df.rename(columns={group_col: "Group"})
    return df

# Build brand-level net sales comparisons for COCO
ftd_brand_net = ftd_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
lw_brand_net = lw_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
l2w_brand_net = l2w_coco_df.groupby("Brand Name", dropna=False)["Net Sales"].sum()
brand_net = build_group_compare(ftd_brand_net, lw_brand_net, l2w_brand_net, "Brand Name")

# Build brand-level discount% comparators
brand_dis = build_group_dis_pct(ftd_coco_df, lw_coco_df, l2w_coco_df, "Brand Name")
# Merge net and dis into one table
brand_comp = pd.merge(brand_net, brand_dis, left_on="Brand Name", right_on="Group", how="left").drop(columns=["Group"])
# rename columns to desired names
brand_comp = brand_comp.rename(columns={
    "FTD":"Net_FTD",
    "LW":"Net_LW",
    "L2W":"Net_L2W",
    "Dis%_FTD":"Dis_FTD",
    "Dis%_LW":"Dis_LW",
    "Dis%_L2W":"Dis_L2W",
    "Change":"Dis_Change"
})
# Reorder columns for final table
brand_comp = brand_comp[[
    "Brand Name",
    "Net_FTD","Net_LW","Growth %","Net_L2W",
    "Dis_FTD","Dis_LW","Dis_Change","Dis_L2W"
]]
brand_comp = brand_comp.fillna(0)

# Region-level
ftd_region_net = ftd_coco_df.assign(RegionNorm=ftd_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
lw_region_net = lw_coco_df.assign(RegionNorm=lw_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
l2w_region_net = l2w_coco_df.assign(RegionNorm=l2w_coco_df["Region"].apply(normalize_region)).groupby("RegionNorm", dropna=False)["Net Sales"].sum()
region_net = build_group_compare(ftd_region_net, lw_region_net, l2w_region_net, "Region")

region_dis = build_group_dis_pct(
    ftd_coco_df.assign(RegionNorm=ftd_coco_df["Region"].apply(normalize_region)),
    lw_coco_df.assign(RegionNorm=lw_coco_df["Region"].apply(normalize_region)),
    l2w_coco_df.assign(RegionNorm=l2w_coco_df["Region"].apply(normalize_region)),
    "RegionNorm"
)
region_comp = pd.merge(region_net, region_dis, left_on="Region", right_on="Group", how="left").drop(columns=["Group"])
region_comp = region_comp.rename(columns={"Region":"Region Name", "Dis%_FTD":"Dis_FTD","Dis%_LW":"Dis_LW","Dis%_L2W":"Dis_L2W","Change":"Dis_Change", "FTD":"Net_FTD","LW":"Net_LW","L2W":"Net_L2W"})
region_comp = region_comp[["Region Name","Net_FTD","Net_LW","Growth %","Net_L2W","Dis_FTD","Dis_LW","Dis_Change","Dis_L2W"]].fillna(0)

# Source-level
ftd_source_net = ftd_coco_df.assign(SourceNorm=ftd_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
lw_source_net = lw_coco_df.assign(SourceNorm=lw_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
l2w_source_net = l2w_coco_df.assign(SourceNorm=l2w_coco_df["Source"].apply(normalize_source)).groupby("SourceNorm", dropna=False)["Net Sales"].sum()
source_net = build_group_compare(ftd_source_net, lw_source_net, l2w_source_net, "Source")

source_dis = build_group_dis_pct(
    ftd_coco_df.assign(SourceNorm=ftd_coco_df["Source"].apply(normalize_source)),
    lw_coco_df.assign(SourceNorm=lw_coco_df["Source"].apply(normalize_source)),
    l2w_coco_df.assign(SourceNorm=l2w_coco_df["Source"].apply(normalize_source)),
    "SourceNorm"
)
source_comp = pd.merge(source_net, source_dis, left_on="Source", right_on="Group", how="left").drop(columns=["Group"])
source_comp = source_comp.rename(columns={"Source":"Source Name", "Dis%_FTD":"Dis_FTD","Dis%_LW":"Dis_LW","Dis%_L2W":"Dis_L2W","Change":"Dis_Change", "FTD":"Net_FTD","LW":"Net_LW","L2W":"Net_L2W"})
source_comp = source_comp[["Source Name","Net_FTD","Net_LW","Growth %","Net_L2W","Dis_FTD","Dis_LW","Dis_Change","Dis_L2W"]].fillna(0)

# Combined: Source + Brand (matrix)
# Pivot Net Sales for COCO MTD by Source and Brand (dates aggregated for MTD)
pivot_source_brand = pd.pivot_table(
    mtd_coco_df.assign(SourceNorm=mtd_coco_df["Source"].apply(normalize_source)),
    values="Net Sales",
    index="SourceNorm",
    columns="Brand Name",
    aggfunc="sum",
    fill_value=0
)

# Combined: Region + Source (MTD)
pivot_region_source = pd.pivot_table(
    mtd_coco_df.assign(RegionNorm=mtd_coco_df["Region"].apply(normalize_region),
                       SourceNorm=mtd_coco_df["Source"].apply(normalize_source)),
    values="Net Sales",
    index="RegionNorm",
    columns="SourceNorm",
    aggfunc="sum",
    fill_value=0
)

# ------------------------------
# Build Email HTML (KPIs on top)
# ------------------------------
def df_to_html_for_email(df, title=None):
    html = ""
    if title:
        html += f"<h3 style='background:#f2f2f2;padding:6px'>{title}</h3>\n"
    html += df.round(2).to_html(border=0, classes="table", index=False)
    return html

# Format numeric columns nicely in the displayed DataFrames
def format_money_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: f"₹{x:,.0f}")
    return df

# Prepare brand_comp display formatting
brand_display = brand_comp.copy()
brand_display["Growth %"] = brand_display["Growth %"].apply(lambda x: f"{x:.1f}%")
brand_display = brand_display.rename(columns={
    "Brand Name":"Brand",
    "Net_FTD":"FTD",
    "Net_LW":"LW",
    "Net_L2W":"L2W",
    "Dis_FTD":"Dis FTD",
    "Dis_LW":"Dis LW",
    "Dis_Change":"Dis Change",
    "Dis_L2W":"Dis L2W"
})
brand_display = brand_display[["Brand","FTD","LW","Growth %","L2W","Dis FTD","Dis LW","Dis Change","Dis L2W"]]
brand_display = format_money_cols(brand_display, ["FTD","LW","L2W"])

region_display = region_comp.copy()
region_display["Growth %"] = region_display["Growth %"].apply(lambda x: f"{x:.1f}%")
region_display = region_display.rename(columns={"Region Name":"Region","Net_FTD":"FTD","Net_LW":"LW","Net_L2W":"L2W","Dis_FTD":"Dis FTD","Dis_LW":"Dis LW","Dis_Change":"Dis Change","Dis_L2W":"Dis L2W"})
region_display = region_display[["Region","FTD","LW","Growth %","L2W","Dis FTD","Dis LW","Dis Change","Dis L2W"]]
region_display = format_money_cols(region_display, ["FTD","LW","L2W"])

source_display = source_comp.copy()
source_display["Growth %"] = source_display["Growth %"].apply(lambda x: f"{x:.1f}%")
source_display = source_display.rename(columns={"Source Name":"Source","Net_FTD":"FTD","Net_LW":"LW","Net_L2W":"L2W","Dis_FTD":"Dis FTD","Dis_LW":"Dis LW","Dis_Change":"Dis Change","Dis_L2W":"Dis L2W"})
source_display = source_display[["Source","FTD","LW","Growth %","L2W","Dis FTD","Dis LW","Dis Change","Dis L2W"]]
source_display = format_money_cols(source_display, ["FTD","LW","L2W"])

# KPI table (top)
kpi_table = pd.DataFrame([{
    "Metric":"Gross",
    "FTD": f"₹{ftd_kpi['Gross']:,.0f}",
    "MTD": f"₹{mtd_kpi['Gross']:,.0f}"
},{
    "Metric":"Net",
    "FTD": f"₹{ftd_kpi['Net']:,.0f}",
    "MTD": f"₹{mtd_kpi['Net']:,.0f}"
},{
    "Metric":"Discount",
    "FTD": f"₹{ftd_kpi['Discount']:,.0f}",
    "MTD": f"₹{mtd_kpi['Discount']:,.0f}"
},{
    "Metric":"Orders",
    "FTD": f"{ftd_kpi['Orders']:,}",
    "MTD": f"{mtd_kpi['Orders']:,}"
},{
    "Metric":"AOV",
    "FTD": f"₹{ftd_kpi['AOV']:,.0f}",
    "MTD": f"₹{mtd_kpi['AOV']:,.0f}"
},{
    "Metric":"Discount %",
    "FTD": f"{ftd_kpi['Dis %']:.1f}%",
    "MTD": f"{mtd_kpi['Dis %']:.1f}%"
}])

# Combine HTML
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
    .kpi-card{{display:inline-block; background:#fff; border:1px solid #e6e6e6; padding:8px; margin:6px; border-radius:4px; width:150px; text-align:center}}
    .kpi-val{{font-weight:bold; font-size:16px; color:#0A7D32}}
  </style>
</head>
<body>
<h2>📊 COCO Dashboard — {ftd_date_only}</h2>

<!-- KPI Top -->
<div>
  <h3>KPIs (COCO only)</h3>
  {kpi_table.to_html(index=False, border=0, classes="table")}
</div>

<!-- Brand Dashboard -->
<div>
  <h3>Brand Dashboard (COCO) — FTD vs LW</h3>
  {brand_display.to_html(index=False, border=0, classes="table")}
</div>

<!-- Region Dashboard -->
<div>
  <h3>Region Dashboard (COCO) — FTD vs LW</h3>
  {region_display.to_html(index=False, border=0, classes="table")}
</div>

<!-- Source Dashboard -->
<div>
  <h3>Source Dashboard (COCO) — FTD vs LW</h3>
  {source_display.to_html(index=False, border=0, classes="table")}
</div>

<!-- Source + Brand (MTD pivot) -->
<div>
  <h3>Source × Brand (MTD, COCO)</h3>
  {pivot_source_brand.round(2).to_html(border=0, classes="table")}
</div>

<!-- Region + Source (MTD pivot) -->
<div>
  <h3>Region × Source (MTD, COCO)</h3>
  {pivot_region_source.round(2).to_html(border=0, classes="table")}
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

# Recipient(s) - adjust if needed
TO = [os.environ.get("RECIPIENTS_TO", "mis2@frozenbottle.in")]

# Send
send_mail(subject=f"COCO Dashboard | {ftd_date_only}", body=html_body, to_addrs=TO)
