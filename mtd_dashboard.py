# =========================================================
# IMPORTS
# =========================================================

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import gspread
import json
import os
import re
from google.oauth2.service_account import Credentials

print("=" * 60)
print("🚀 MTD DASHBOARD STARTED")
print("=" * 60)


# ---------------- GOOGLE ---------------- #


creds = Credentials.from_service_account_info(
    json.loads(
        os.environ["GOOGLE_CREDENTIALS"]
    ),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

sheet_url = "https://docs.google.com/spreadsheets/d/1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY/edit"

spreadsheet = client.open_by_url(sheet_url)

print("✅ Connected to Google Sheet")

# =========================================================
# PUSH DATAFRAME TO GOOGLE SHEET
# =========================================================

def push(sheet_name, df):

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except:
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows="5000",
            cols="30"
        )

    ws.clear()

    data = [df.columns.tolist()] + df.fillna("").values.tolist()

    ws.update(data)

    print(f"✅ Updated : {sheet_name}")

# =========================================================
# FIND LATEST MONTHLY CSV
# =========================================================

BASE_FOLDER = Path("monthly_data")

csv_files = list(
    BASE_FOLDER.rglob("*.csv")
)

print(f"📂 CSV Files Found : {len(csv_files)}")

if not csv_files:
    raise Exception("❌ No Monthly CSV Files Found")

# =========================================================
# PICK LATEST FILE BY FILE NAME
# =========================================================

month_map = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

def extract_month_year(path):

    m = re.search(r"MTD_(\w{3})_(\d{2})\.csv", path.name)

    if not m:
        return datetime(1900, 1, 1)

    month = month_map[m.group(1)]
    year = 2000 + int(m.group(2))

    return datetime(year, month, 1)


latest_csv = max(
    csv_files,
    key=extract_month_year
)

print(f"📄 Latest File : {latest_csv}")

# =========================================================
# READ CSV
# =========================================================

final_df = pd.read_csv(
    latest_csv,
    low_memory=False
)

# =========================================================
# CLEAN DATA
# =========================================================

final_df["Date"] = pd.to_datetime(final_df["Date"])

final_df["Store Type"] = (
    final_df["Store Type"]
    .fillna("")
    .astype(str)
    .str.strip()
    .str.upper()
)

print("=" * 60)
print("TOTAL ROWS :", len(final_df))
print("TOTAL COLUMNS :", len(final_df.columns))
print("=" * 60)

print(final_df.head())

print("=" * 60)
print("DATE RANGE CHECK")
print("=" * 60)

print("Min Date :", final_df["Date"].min())
print("Max Date :", final_df["Date"].max())

print()

print(final_df["Date"].value_counts().sort_index())

print("=" * 60)
print("STORE TYPE VALUES (FULL DATA)")
print("=" * 60)

print(final_df["Store Type"].value_counts(dropna=False))

# =========================================================
# BUSINESS DATE
# =========================================================

available_dates = sorted(final_df["Date"].dropna().unique())

today = available_dates[-1]

def nearest_available(target_date):
    valid = [d for d in available_dates if d <= target_date]
    return valid[-1] if valid else None

last_week = nearest_available(today - timedelta(days=7))
last_2_week = nearest_available(today - timedelta(days=14))
last_month = nearest_available(today - pd.DateOffset(months=1))
last_year = nearest_available(today - pd.DateOffset(years=1))

print("=" * 60)
print("DATE CHECK")
print("=" * 60)

print("Today      :", today.date())
print("Last Week  :", last_week.date() if last_week is not None else "NA")
print("Last 2 Week:", last_2_week.date() if last_2_week is not None else "NA")
print("Last Month :", last_month.date() if last_month is not None else "NA")
print("Last Year  :", last_year.date() if last_year is not None else "NA")

# =========================================================
# FILTER DATA
# =========================================================

today_df = final_df[final_df["Date"] == today].copy()

lw_df = (
    final_df[final_df["Date"] == last_week].copy()
    if last_week is not None else pd.DataFrame(columns=final_df.columns)
)

l2w_df = (
    final_df[final_df["Date"] == last_2_week].copy()
    if last_2_week is not None else pd.DataFrame(columns=final_df.columns)
)

mom_df = (
    final_df[final_df["Date"] == last_month].copy()
    if last_month is not None else pd.DataFrame(columns=final_df.columns)
)

ly_df = (
    final_df[final_df["Date"] == last_year].copy()
    if last_year is not None else pd.DataFrame(columns=final_df.columns)
)

print("=" * 60)
print("FILTER CHECK")
print("=" * 60)

print("Today Rows :", len(today_df))
print("LW Rows    :", len(lw_df))
print("L2W Rows   :", len(l2w_df))
print("MoM Rows   :", len(mom_df))
print("LY Rows    :", len(ly_df))

# =========================================================
# STORE TYPE SPLIT
# =========================================================

pan_df = today_df.copy()

coco_df = today_df[
    today_df["Store Type"] == "COCO"
].copy()

fofo_df = today_df[
    today_df["Store Type"] == "FOFO"
].copy()

print("=" * 60)
print("STORE TYPE VALUES (TODAY)")
print("=" * 60)

print(today_df["Store Type"].value_counts(dropna=False))

print("=" * 60)
print("STORE TYPE SPLIT")
print("=" * 60)

print("PAN INDIA :", len(pan_df))
print("COCO      :", len(coco_df))
print("FOFO      :", len(fofo_df))

# =========================================================
# UNIVERSAL SUMMARY BUILDER
# =========================================================

def build_summary(df, column):

    summary = (
        df.groupby(column)
        .agg(
            Gross=("Gross Sales", "sum"),
            Net=("Net Sales", "sum"),
            Discount=("Discount", "sum"),
            Orders=("Orders", "sum")
        )
        .reset_index()
    )

    summary["AOV"] = (
        summary["Net"]
        /
        summary["Orders"].replace(0, 1)
    )

    summary["Dis %"] = (
        summary["Discount"]
        /
        summary["Gross"].replace(0, 1)
    ) * 100

    summary["Contribution %"] = (
        summary["Net"]
        /
        summary["Net"].sum()
    ) * 100

    summary = summary.sort_values(
        "Net",
        ascending=False
    ).reset_index(drop=True)

    return summary

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


pan_kpi = get_kpi(pan_df)
coco_kpi = get_kpi(coco_df)
fofo_kpi = get_kpi(fofo_df)
lw_kpi = get_kpi(lw_df)
l2w_kpi = get_kpi(l2w_df)
mom_kpi = get_kpi(mom_df)
ly_kpi = get_kpi(ly_df)

print("="*95)
print("PAN INDIA vs COCO vs FOFO")
print("="*95)

# =========================================================
# EMAIL DATA
# =========================================================

compare_df = pd.DataFrame({

    "Metric":[
        "Gross",
        "Net",
        "Discount",
        "Orders",
        "Qty",
        "AOV",
        "Dis %"
    ],

    "PAN INDIA":[
        pan_kpi["Gross"],
        pan_kpi["Net"],
        pan_kpi["Discount"],
        pan_kpi["Orders"],
        pan_kpi["Qty"],
        pan_kpi["AOV"],
        pan_kpi["Dis %"]
    ],

    "COCO":[
        coco_kpi["Gross"],
        coco_kpi["Net"],
        coco_kpi["Discount"],
        coco_kpi["Orders"],
        coco_kpi["Qty"],
        coco_kpi["AOV"],
        coco_kpi["Dis %"]
    ],

    "FOFO":[
        fofo_kpi["Gross"],
        fofo_kpi["Net"],
        fofo_kpi["Discount"],
        fofo_kpi["Orders"],
        fofo_kpi["Qty"],
        fofo_kpi["AOV"],
        fofo_kpi["Dis %"]
    ]

}).round(2)

print(compare_df)

# =========================================================
# GENERIC GROWTH SUMMARY
# =========================================================

def growth_summary(current_df, previous_df, group_col, period_name):

    curr = (
        current_df
        .groupby(group_col)
        .agg(
            Gross=("Gross Sales","sum"),
            Net=("Net Sales","sum"),
            Orders=("Orders","sum")
        )
        .reset_index()
    )

    prev = (
        previous_df
        .groupby(group_col)
        .agg(
            Gross_Previous=("Gross Sales","sum"),
            Net_Previous=("Net Sales","sum"),
            Orders_Previous=("Orders","sum")
        )
        .reset_index()
    )

    df = curr.merge(
        prev,
        on=group_col,
        how="left"
    ).fillna(0)

    df[f"{period_name} Gross %"] = (
        (df["Gross"] - df["Gross_Previous"])
        /
        df["Gross_Previous"].replace(0,1)
    ) * 100

    df[f"{period_name} Net %"] = (
        (df["Net"] - df["Net_Previous"])
        /
        df["Net_Previous"].replace(0,1)
    ) * 100

    df[f"{period_name} Orders %"] = (
        (df["Orders"] - df["Orders_Previous"])
        /
        df["Orders_Previous"].replace(0,1)
    ) * 100

    return df.round(2)

# =========================================================
# UNIVERSAL GROWTH CALCULATOR
# =========================================================

def add_growth(current_df, compare_df, column, suffix):

    compare = (
        compare_df.groupby(column)
        .agg(
            Compare_Net=("Net Sales", "sum")
        )
        .reset_index()
    )

    current_df = current_df.merge(
        compare,
        on=column,
        how="left"
    )

    current_df["Compare_Net"] = (
        current_df["Compare_Net"]
        .fillna(0)
    )

    current_df[f"{suffix} Growth %"] = (
        (
            current_df["Net"]
            - current_df["Compare_Net"]
        )
        /
        current_df["Compare_Net"].replace(0, pd.NA)
    ) * 100

    current_df[f"{suffix} Growth %"] = (
        current_df[f"{suffix} Growth %"]
        .fillna(0)
        .round(1)
    )

    current_df.drop(
        columns="Compare_Net",
        inplace=True
    )

    return current_df

print("="*60)
print("LW Dates")
print("="*60)
print(lw_df["Date"].value_counts())

print("="*60)
print("L2W Dates")
print("="*60)
print(l2w_df["Date"].value_counts())

print("LW Net :", lw_df["Net Sales"].sum())
print("L2W Net:", l2w_df["Net Sales"].sum())

# =========================================================
# TODAY BRAND SUMMARY
# =========================================================

brand_summary = build_summary(
    coco_df,
    "Brand Name"
)

print("=" * 60)
print("TODAY BRAND SUMMARY")
print("=" * 60)
print(brand_summary.round(2))

push(
    "Dashboard_Brand",
    brand_summary.round(2)
)

# =========================================================
# BRAND LW
# =========================================================

brand_lw = add_growth(
    brand_summary.copy(),
    lw_df,
    "Brand Name",
    "LW"
)

print("=" * 60)
print("BRAND LW GROWTH")
print("=" * 60)
print(brand_lw.round(2))

push(
    "Dashboard_Brand_LW",
    brand_lw.round(2)
)

# =========================================================
# BRAND L2W
# =========================================================

brand_l2w = add_growth(
    brand_summary.copy(),
    l2w_df,
    "Brand Name",
    "L2W"
)

print("=" * 60)
print("BRAND L2W GROWTH")
print("=" * 60)
print(brand_l2w.round(2))

push(
    "Dashboard_Brand_L2W",
    brand_l2w.round(2)
)

source_summary = build_summary(
    coco_df,
    "Source"
)

push(
    "Dashboard_Source",
    source_summary.round(2)
)

source_lw = add_growth(
    source_summary.copy(),
    lw_df,
    "Source",
    "LW"
)

push(
    "Dashboard_Source_LW",
    source_lw.round(2)
)

print(source_lw.round(2))

source_l2w = add_growth(
    source_summary.copy(),
    l2w_df,
    "Source",
    "L2W"
)

push(
    "Dashboard_Source_L2W",
    source_l2w.round(2)
)

print(source_l2w.round(2))

# =========================================================
# TODAY BRANCH SUMMARY
# =========================================================

branch_summary = build_summary(
    coco_df,
    "Branch"
)

push(
    "Dashboard_Branch",
    branch_summary.round(2)
)

# =========================================================
# TOP 10 BRANCHES FOR EMAIL
# =========================================================

top_branch_df = (
    branch_summary
    .sort_values("Net", ascending=False)
    .head(10)
)

branch_lw = add_growth(
    branch_summary.copy(),
    lw_df,
    "Branch",
    "LW"
)

push(
    "Dashboard_Branch_LW",
    branch_lw.round(2)
)

print(branch_lw.round(2))



branch_l2w = add_growth(
    branch_summary.copy(),
    l2w_df,
    "Branch",
    "L2W"
)

push(
    "Dashboard_Branch_L2W",
    branch_l2w.round(2)
)

print(branch_l2w.round(2))
# =========================================================
# TODAY SESSION SUMMARY
# =========================================================

session_summary = build_summary(
    coco_df,
    "Session"
)

# ---------------------------------------------
# Replace Blank Sessions
# ---------------------------------------------
session_summary["Session"] = (
    session_summary["Session"]
    .fillna("Others")
    .replace("", "Others")
)

# ---------------------------------------------
# Session Sort Order
# ---------------------------------------------
session_order = {
    "Breakfast": 1,
    "Lunch": 2,
    "Snacks": 3,
    "Dinner": 4,
    "Late Night": 5,
    "Others": 99
}

session_summary["Sort"] = (
    session_summary["Session"]
    .map(session_order)
    .fillna(99)
)

session_summary = (
    session_summary
    .sort_values("Sort")
    .drop(columns="Sort")
    .reset_index(drop=True)
)

print("=" * 60)
print("TODAY SESSION SUMMARY")
print("=" * 60)

print(session_summary.round(2))

push(
    "Dashboard_Session",
    session_summary.round(2)
)

# =========================================================
# SESSION LW GROWTH
# =========================================================

session_lw = add_growth(
    session_summary.copy(),
    lw_df,
    "Session",
    "LW"
)

print("=" * 60)
print("SESSION LW GROWTH")
print("=" * 60)

print(session_lw.round(2))

push(
    "Dashboard_Session_LW",
    session_lw.round(2)
)


# =========================================================
# SESSION L2W GROWTH
# =========================================================

session_l2w = add_growth(
    session_summary.copy(),
    l2w_df,
    "Session",
    "L2W"
)

print("=" * 60)
print("SESSION L2W GROWTH")
print("=" * 60)

print(session_l2w.round(2))

push(
    "Dashboard_Session_L2W",
    session_l2w.round(2)
)

# Region Summary

region_summary = build_summary(
    coco_df,
    "Region"
)

push(
    "Dashboard_Region",
    region_summary.round(2)
)

region_lw = add_growth(
    region_summary.copy(),
    lw_df,
    "Region",
    "LW"
)

push(
    "Dashboard_Region_LW",
    region_lw.round(2)
)

print(region_lw.round(2))

region_l2w = add_growth(
    region_summary.copy(),
    l2w_df,
    "Region",
    "L2W"
)

push(
    "Dashboard_Region_L2W",
    region_l2w.round(2)
)

print(region_l2w.round(2))

kpi_df = pd.DataFrame({

    "KPI":[
        "Gross Revenue",
        "Net Revenue",
        "Discount",
        "Orders",
        "AOV",
        "Discount %"
    ],

    "Value":[

        pan_kpi["Gross"],
        pan_kpi["Net"],
        pan_kpi["Discount"],
        pan_kpi["Orders"],
        pan_kpi["AOV"],
        pan_kpi["Dis %"]

    ]

})

# Send Mail
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# =========================================================
# EMAIL CONFIGURATION
# =========================================================

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")

TO = [
    "mis2@frozenbottle.in"
]

def html_table(df):

    return (
        df.round(2)
        .to_html(
            index=False,
            classes="table",
            border=0
        )
    )

body = f"""

<html>

<head>

<style>

body{{
font-family:Calibri;
font-size:14px;
background:#F5F7FA;
}}

h2{{
background:#243447;
color:white;
padding:10px;
}}

h3{{
background:#2E8B57;
color:white;
padding:6px;
}}

.table{{
border-collapse:collapse;
width:100%;
margin-bottom:25px;
}}

.table th{{
background:#243447;
color:white;
padding:8px;
text-align:center;
}}

.table td{{
padding:6px;
text-align:center;
border:1px solid #dddddd;
}}

.table tr:nth-child(even){{
background:#f5f5f5;
}}

.kpi{{
display:flex;
gap:20px;
margin-bottom:20px;
}}

.card{{
background:white;
padding:12px;
border-radius:8px;
box-shadow:0 2px 5px #ccc;
width:180px;
text-align:center;
}}

.value{{
font-size:22px;
font-weight:bold;
color:#0A7D32;
}}

</style>

</head>

<body>

<h2>
📊 MTD Dashboard
<br>
{today.strftime("%d-%b-%Y")}
</h2>

<div class="kpi">

<div class="card">
Gross
<div class="value">
₹{pan_kpi["Gross"]:,.0f}
</div>
</div>

<div class="card">
Net
<div class="value">
₹{pan_kpi["Net"]:,.0f}
</div>
</div>

<div class="card">
Orders
<div class="value">
{int(pan_kpi["Orders"]):,}
</div>
</div>

<div class="card">
AOV
<div class="value">
₹{pan_kpi["AOV"]:,.0f}
</div>
</div>

<div class="card">
Discount
<div class="value">
{pan_kpi["Dis %"]:.1f}%
</div>
</div>

</div>

<h3>KPI Summary</h3>

{html_table(kpi_df)}

<h3>COCO vs FOFO</h3>

{html_table(compare_df)}

<h3>Brand Summary</h3>

{html_table(brand_summary)}

<h3>Source Summary</h3>

{html_table(source_summary)}

<h3>Region Summary</h3>

{html_table(region_summary)}

<h3>Session Summary</h3>

{html_table(session_summary)}

<h3>Top 10 Branches</h3>

{html_table(top_branch_df)}

</body>

</html>

"""
def send_mail(subject, body):

    msg = MIMEMultipart()

    msg["From"] = EMAIL
    msg["To"] = ",".join(TO)
    msg["Subject"] = subject

    msg.attach(
        MIMEText(
            body,
            "html"
        )
    )

    server = smtplib.SMTP(
        SMTP_SERVER,
        SMTP_PORT
    )

    server.starttls()

    server.login(
        EMAIL,
        PASSWORD
    )

    server.sendmail(
        EMAIL,
        TO,
        msg.as_string()
    )

    server.quit()

    print("✅ Mail Sent")

send_mail(

    subject=f"MTD Dashboard | {today.strftime('%d-%b-%Y')}",

    body=body

)


