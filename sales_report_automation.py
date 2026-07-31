import os
import json
import time
import jwt
import requests
import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Starting Fast Multi-Threaded Sales Report Automation")

# =========================================================
# 1. AUTHENTICATION & ENVIRONMENT
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]
GSHEET_KEY = "1PhVeFoPERJODrGPW68ORAO0kJjToB5ZYMY-yOkGTWDk"
SHEET_LINK = f"https://docs.google.com/spreadsheets/d/{GSHEET_KEY}/edit"

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
RECIPIENTS_TO = os.environ.get("RECIPIENTS_TO", "mis2@frozenbottle.in")
RECIPIENTS_CC = os.environ.get("RECIPIENTS_CC", "")

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def get_headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# Connect Google Sheets
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(GSHEET_KEY)
print("✅ Connected to Google Sheet")

# =========================================================
# 2. DATE RANGES CALCULATION
# =========================================================
today = datetime.now()
yesterday = today - timedelta(days=1)
ftd_date_str = yesterday.strftime("%Y-%m-%d")

# MTD: 1st to Yesterday
mtd_start = yesterday.replace(day=1)
mtd_end = yesterday

# pMTD: 1st of previous month to same day
if mtd_start.month == 1:
    pmtd_start = mtd_start.replace(year=mtd_start.year - 1, month=12, day=1)
else:
    pmtd_start = mtd_start.replace(month=mtd_start.month - 1, day=1)

import calendar
_, max_days_pm = calendar.monthrange(pmtd_start.year, pmtd_start.month)
pmtd_day = min(yesterday.day, max_days_pm)
pmtd_end = pmtd_start.replace(day=pmtd_day)

mtd_label = mtd_end.strftime("%b-%y")
pmtd_label = pmtd_end.strftime("%b-%y")

print(f"📅 FTD Date: {ftd_date_str}")
print(f"📅 MTD Range: {mtd_start.strftime('%Y-%m-%d')} to {mtd_end.strftime('%Y-%m-%d')} ({mtd_label})")
print(f"📅 pMTD Range: {pmtd_start.strftime('%Y-%m-%d')} to {pmtd_end.strftime('%Y-%m-%d')} ({pmtd_label})")

# =========================================================
# 3. FETCH STORES FROM HELP SHEET
# =========================================================
try:
    help_ws = spreadsheet.worksheet("Help Sheet")
    help_df = pd.DataFrame(help_ws.get_all_records())
    help_df.columns = help_df.columns.str.strip()
    
    branch_col = None
    for col in help_df.columns:
        if col.lower().replace(" ", "") in ["branchcode", "storecode", "code"]:
            branch_col = col
            break
            
    if not branch_col:
        branch_col = help_df.columns[0]
        
    valid_branches = help_df[branch_col].dropna().astype(str).str.strip().unique().tolist()
    print(f"✅ Loaded {len(valid_branches)} store branch codes from Help Sheet.")
except Exception as e:
    print(f"❌ Error loading Help Sheet: {e}")
    exit(1)

# =========================================================
# 4. PARALLEL API DATA FETCHING
# =========================================================
sales_url = "https://api.ristaapps.com/v1/sales/page"

def fetch_single_branch_day(branch, day_str, curr_dt):
    rows = []
    params = {"branch": branch, "day": day_str}
    try:
        res = requests.get(sales_url, headers=get_headers(), params=params, timeout=30)
        if res.status_code == 200:
            data = res.json().get("data", [])
            for order in data:
                channel = order.get("Channel") or order.get("channel") or "Ownly"
                channel_str = str(channel).lower()
                if "swiggy" in channel_str:
                    platform_group = "Swiggy"
                elif "zomato" in channel_str:
                    platform_group = "Zomato"
                else:
                    platform_group = "Ownly"

                items = order.get("items") or order.get("orderItems") or [order]
                for item in items:
                    brand_name = (
                        item.get("brandName") or 
                        order.get("brandName") or 
                        item.get("brand") or 
                        order.get("brand") or 
                        "Frozen Bottle"
                    )
                    category = (
                        item.get("item_categoryName") or 
                        item.get("categoryName") or 
                        item.get("category") or 
                        "General"
                    )
                    item_name = (
                        item.get("item_longName") or
                        item.get("item_shortName") or 
                        item.get("shortName") or 
                        item.get("itemName") or 
                        item.get("name") or 
                        "Unknown"
                    )
                    
                    # 🛠️ IMPROVED ITEM VARIANT EXTRACTION
                    raw_variant = (
                        item.get("item_variant") or
                        item.get("item_variants") or 
                        item.get("variantName") or 
                        item.get("variant") or 
                        item.get("variation") or 
                        "Regular"
                    )
                    variant = str(raw_variant).strip()
                    if variant in ["", "nan", "None", "-", "null"]:
                        variant = "Regular"

                    qty = float(item.get("item_quantity") or item.get("quantity") or item.get("qty") or 0)
                    gross = float(item.get("item_grossAmount") or item.get("grossAmount") or item.get("gross") or 0)
                    discount = abs(float(
                        item.get("item_netDiscountAmount") or 
                        item.get("netDiscountAmount") or 
                        item.get("discount") or 
                        item.get("discounts") or 0
                    ))
                    net = float(item.get("item_netAmount") or item.get("netAmount") or item.get("net") or 0)
                    materials = float(
                        item.get("item_itemMaterialCost") or 
                        item.get("itemMaterialCost") or 
                        item.get("materialCost") or 
                        item.get("materials") or 0
                    )

                    is_ftd = (day_str == ftd_date_str)

                    rows.append({
                        "Date": day_str,
                        "Month": curr_dt.strftime("%b-%y"),
                        "Period": "MTD" if curr_dt >= mtd_start else "pMTD",
                        "Is_FTD": is_ftd,
                        "Brand Name": brand_name,
                        "Category": category,
                        "Item Name": item_name,
                        "Item Variant": variant,
                        "Platform Group": platform_group,
                        "Gross": gross,
                        "Discounts": discount,
                        "Net": net,
                        "Materials": materials,
                        "Qty": qty
                    })
    except Exception:
        pass
    return rows

def fetch_sales_data_parallel(start_dt, end_dt, max_workers=15):
    all_rows = []
    tasks = []
    curr_dt = start_dt
    while curr_dt <= end_dt:
        day_str = curr_dt.strftime("%Y-%m-%d")
        for branch in valid_branches:
            tasks.append((branch, day_str, curr_dt))
        curr_dt += timedelta(days=1)
        
    print(f"⚡ Queueing {len(tasks)} parallel API requests...")
    start_time = time.time()
    completed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(fetch_single_branch_day, b, d, dt): (b, d) for b, d, dt in tasks
        }
        for future in as_completed(future_to_task):
            res = future.result()
            if res:
                all_rows.extend(res)
            completed += 1
            if completed % 250 == 0 or completed == len(tasks):
                print(f"   Progress: {completed}/{len(tasks)} requests completed...")
                
    print(f"⏱️ Fetch completed in {round(time.time() - start_time, 1)} seconds.")
    return pd.DataFrame(all_rows)

df_pmtd = fetch_sales_data_parallel(pmtd_start, pmtd_end, max_workers=15)
df_mtd = fetch_sales_data_parallel(mtd_start, mtd_end, max_workers=15)

df_all = pd.concat([df_pmtd, df_mtd], ignore_index=True)

if df_all.empty:
    print("❌ No data fetched. Exiting.")
    exit()

# Clean Variants Across Master DataFrame
df_all['Item Variant'] = df_all['Item Variant'].fillna('Regular').astype(str).str.strip()
df_all['Item Variant'] = df_all['Item Variant'].replace(['', 'nan', 'None', '-', 'NaN'], 'Regular')

# Current Month Live Items Filter
live_items = df_all[(df_all['Period'] == 'MTD') & (df_all['Net'] > 0)]['Item Name'].unique()
df_all = df_all[df_all['Item Name'].isin(live_items)].copy()
print(f"✅ Active Live Items Filtered: {len(live_items)} items.")

# =========================================================
# 5. DATA AGGREGATION BUILDERS
# =========================================================
def build_report_matrix(df, group_cols):
    if df.empty:
        return pd.DataFrame()

    agg_df = df.groupby(['Period', 'Platform Group'] + group_cols).agg(
        Gross=('Gross', 'sum'),
        Discounts=('Discounts', 'sum'),
        Net=('Net', 'sum'),
        Materials=('Materials', 'sum'),
        Qty=('Qty', 'sum')
    ).reset_index()

    res_rows = []
    unique_items = agg_df[group_cols].drop_duplicates()
    
    for _, row_keys in unique_items.iterrows():
        sub = agg_df
        for col in group_cols:
            sub = sub[sub[col] == row_keys[col]]
            
        row_dict = row_keys.to_dict()
        
        pmtd_gross = sub[sub['Period'] == 'pMTD']['Gross'].sum()
        mtd_gross = sub[sub['Period'] == 'MTD']['Gross'].sum()
        
        pmtd_disc = sub[sub['Period'] == 'pMTD']['Discounts'].sum()
        mtd_disc = sub[sub['Period'] == 'MTD']['Discounts'].sum()
        
        pmtd_net = sub[sub['Period'] == 'pMTD']['Net'].sum()
        mtd_net = sub[sub['Period'] == 'MTD']['Net'].sum()
        
        pmtd_mat = sub[sub['Period'] == 'pMTD']['Materials'].sum()
        mtd_mat = sub[sub['Period'] == 'MTD']['Materials'].sum()
        
        row_dict[f'Overall Sales ({pmtd_label})'] = round(pmtd_net, 2)
        row_dict[f'Overall Sales ({mtd_label})'] = round(mtd_net, 2)
        row_dict['Overall Growth %'] = round((mtd_net - pmtd_net) / pmtd_net, 4) if pmtd_net > 0 else 0
        
        row_dict[f'Dis% ({pmtd_label})'] = round(pmtd_disc / pmtd_gross, 4) if pmtd_gross > 0 else 0
        row_dict[f'Dis% ({mtd_label})'] = round(mtd_disc / mtd_gross, 4) if mtd_gross > 0 else 0
        row_dict['Dis% Growth %'] = round(row_dict[f'Dis% ({mtd_label})'] - row_dict[f'Dis% ({pmtd_label})'], 4)
        
        row_dict[f'Food Cost % ({pmtd_label})'] = round(pmtd_mat / pmtd_net, 4) if pmtd_net > 0 else 0
        row_dict[f'Food Cost % ({mtd_label})'] = round(mtd_mat / mtd_net, 4) if mtd_net > 0 else 0
        row_dict['Food Cost % Growth %'] = round(row_dict[f'Food Cost % ({mtd_label})'] - row_dict[f'Food Cost % ({pmtd_label})'], 4)
        
        for channel_name in ['Ownly', 'Swiggy', 'Zomato']:
            prefix = "In Store" if channel_name == "Ownly" else channel_name
            c_sub = sub[sub['Platform Group'] == channel_name]
            
            c_pmtd_net = c_sub[c_sub['Period'] == 'pMTD']['Net'].sum()
            c_mtd_net = c_sub[c_sub['Period'] == 'MTD']['Net'].sum()
            
            c_pmtd_gross = c_sub[c_sub['Period'] == 'pMTD']['Gross'].sum()
            c_mtd_gross = c_sub[c_sub['Period'] == 'MTD']['Gross'].sum()
            
            c_pmtd_disc = c_sub[c_sub['Period'] == 'pMTD']['Discounts'].sum()
            c_mtd_disc = c_sub[c_sub['Period'] == 'MTD']['Discounts'].sum()
            
            c_pmtd_mat = c_sub[c_sub['Period'] == 'pMTD']['Materials'].sum()
            c_mtd_mat = c_sub[c_sub['Period'] == 'MTD']['Materials'].sum()
            
            row_dict[f'{prefix} Sales ({pmtd_label})'] = round(c_pmtd_net, 2)
            row_dict[f'{prefix} Sales ({mtd_label})'] = round(c_mtd_net, 2)
            row_dict[f'{prefix} Sales Growth %'] = round((c_mtd_net - c_pmtd_net) / c_pmtd_net, 4) if c_pmtd_net > 0 else 0
            
            row_dict[f'{prefix} Dis% ({pmtd_label})'] = round(c_pmtd_disc / c_pmtd_gross, 4) if c_pmtd_gross > 0 else 0
            row_dict[f'{prefix} Dis% ({mtd_label})'] = round(c_mtd_disc / c_mtd_gross, 4) if c_mtd_gross > 0 else 0
            row_dict[f'{prefix} Dis% Growth %'] = round(row_dict[f'{prefix} Dis% ({mtd_label})'] - row_dict[f'{prefix} Dis% ({pmtd_label})'], 4)
            
            row_dict[f'{prefix} Food Cost % ({pmtd_label})'] = round(c_pmtd_mat / c_pmtd_net, 4) if c_pmtd_net > 0 else 0
            row_dict[f'{prefix} Food Cost % ({mtd_label})'] = round(c_mtd_mat / c_mtd_net, 4) if c_mtd_net > 0 else 0
            row_dict[f'{prefix} Food Cost % Growth %'] = round(row_dict[f'{prefix} Food Cost % ({mtd_label})'] - row_dict[f'{prefix} Food Cost % ({pmtd_label})'], 4)

        res_rows.append(row_dict)
        
    return pd.DataFrame(res_rows)

def build_ftd_mtd_tab(df):
    df_mtd_only = df[df['Period'] == 'MTD']
    if df_mtd_only.empty:
        return pd.DataFrame()

    unique_items = df_mtd_only[['Brand Name', 'Category', 'Item Name', 'Item Variant']].drop_duplicates()
    res_rows = []

    for _, row_keys in unique_items.iterrows():
        b_name, c_name, i_name, v_name = row_keys['Brand Name'], row_keys['Category'], row_keys['Item Name'], row_keys['Item Variant']
        sub = df_mtd_only[(df_mtd_only['Brand Name'] == b_name) & 
                          (df_mtd_only['Category'] == c_name) & 
                          (df_mtd_only['Item Name'] == i_name) &
                          (df_mtd_only['Item Variant'] == v_name)]

        ftd_sub = sub[sub['Is_FTD'] == True]

        row_dict = {
            "Brand Name": b_name,
            "Category Name": c_name,
            "Item Name": i_name,
            "Item Variant": v_name,
            "Overall FTD Sales": round(ftd_sub['Net'].sum(), 2),
            "Overall MTD Sales": round(sub['Net'].sum(), 2),
            "Overall FTD Dis%": round(ftd_sub['Discounts'].sum() / ftd_sub['Gross'].sum(), 4) if ftd_sub['Gross'].sum() > 0 else 0,
            "Overall MTD Dis%": round(sub['Discounts'].sum() / sub['Gross'].sum(), 4) if sub['Gross'].sum() > 0 else 0,
            "Overall FTD FC%": round(ftd_sub['Materials'].sum() / ftd_sub['Net'].sum(), 4) if ftd_sub['Net'].sum() > 0 else 0,
            "Overall MTD FC%": round(sub['Materials'].sum() / sub['Net'].sum(), 4) if sub['Net'].sum() > 0 else 0,
        }

        for ch_key, ch_label in [("Ownly", "In Store"), ("Swiggy", "Swiggy"), ("Zomato", "Zomato")]:
            c_sub, c_ftd_sub = sub[sub['Platform Group'] == ch_key], ftd_sub[ftd_sub['Platform Group'] == ch_key]
            row_dict[f"{ch_label} FTD Sales"] = round(c_ftd_sub['Net'].sum(), 2)
            row_dict[f"{ch_label} MTD Sales"] = round(c_sub['Net'].sum(), 2)
            row_dict[f"{ch_label} FTD Dis%"] = round(c_ftd_sub['Discounts'].sum() / c_ftd_sub['Gross'].sum(), 4) if c_ftd_sub['Gross'].sum() > 0 else 0
            row_dict[f"{ch_label} MTD Dis%"] = round(c_sub['Discounts'].sum() / c_sub['Gross'].sum(), 4) if c_sub['Gross'].sum() > 0 else 0
            row_dict[f"{ch_label} FTD FC%"] = round(c_ftd_sub['Materials'].sum() / c_ftd_sub['Net'].sum(), 4) if c_ftd_sub['Net'].sum() > 0 else 0
            row_dict[f"{ch_label} MTD FC%"] = round(c_sub['Materials'].sum() / c_sub['Net'].sum(), 4) if c_sub['Net'].sum() > 0 else 0

        res_rows.append(row_dict)

    return pd.DataFrame(res_rows)

def build_performance_tab(df):
    """Generates Brand-wise Top 10 and Bottom 10 Performers based on MTD Sales."""
    df_mtd_only = df[df['Period'] == 'MTD']
    if df_mtd_only.empty:
        return pd.DataFrame()

    item_summary = df_mtd_only.groupby(['Brand Name', 'Category', 'Item Name', 'Item Variant']).agg(
        Net_Sales=('Net', 'sum'),
        Gross_Sales=('Gross', 'sum'),
        Discounts=('Discounts', 'sum'),
        Materials=('Materials', 'sum'),
        Qty=('Qty', 'sum')
    ).reset_index()

    item_summary['Dis%'] = round(item_summary['Discounts'] / item_summary['Gross_Sales'], 4).fillna(0)
    item_summary['Food Cost %'] = round(item_summary['Materials'] / item_summary['Net_Sales'], 4).fillna(0)

    perf_rows = []
    brands = item_summary['Brand Name'].unique()

    for b in brands:
        b_sub = item_summary[item_summary['Brand Name'] == b].sort_values(by='Net_Sales', ascending=False)
        
        top10 = b_sub.head(10).copy()
        top10['Performance Rank'] = [f"Top {i+1}" for i in range(len(top10))]
        
        bot10 = b_sub.tail(10).copy().sort_values(by='Net_Sales', ascending=True)
        bot10['Performance Rank'] = [f"Bottom {i+1}" for i in range(len(bot10))]
        
        comb = pd.concat([top10, bot10], ignore_index=True)
        for _, r in comb.iterrows():
            perf_rows.append({
                "Brand Name": r['Brand Name'],
                "Performance Rank": r['Performance Rank'],
                "Category": r['Category'],
                "Item Name": r['Item Name'],
                "Item Variant": r['Item Variant'],
                "MTD Net Sales": round(r['Net_Sales'], 2),
                "MTD Qty Sold": int(r['Qty']),
                "Dis%": r['Dis%'],
                "Food Cost %": r['Food Cost %']
            })

    return pd.DataFrame(perf_rows)

def build_insights_tab(df):
    """Generates High Food Cost (>35%) and High Discount (>25%) Actionable Insights."""
    df_mtd_only = df[df['Period'] == 'MTD']
    if df_mtd_only.empty:
        return pd.DataFrame()

    item_summary = df_mtd_only.groupby(['Brand Name', 'Category', 'Item Name', 'Item Variant']).agg(
        Net_Sales=('Net', 'sum'),
        Gross_Sales=('Gross', 'sum'),
        Discounts=('Discounts', 'sum'),
        Materials=('Materials', 'sum')
    ).reset_index()

    item_summary['Dis%'] = round(item_summary['Discounts'] / item_summary['Gross_Sales'], 4).fillna(0)
    item_summary['Food Cost %'] = round(item_summary['Materials'] / item_summary['Net_Sales'], 4).fillna(0)

    insights_rows = []

    # High Food Cost Alert (>35%)
    high_fc = item_summary[item_summary['Food Cost %'] > 0.35].sort_values(by='Food Cost %', ascending=False)
    for _, r in high_fc.iterrows():
        insights_rows.append({
            "Alert Category": "⚠️ High Food Cost (>35%)",
            "Brand Name": r['Brand Name'],
            "Category": r['Category'],
            "Item Name": r['Item Name'],
            "Item Variant": r['Item Variant'],
            "Metric Value": f"{round(r['Food Cost %']*100, 2)}%",
            "MTD Net Sales": round(r['Net_Sales'], 2),
            "Action Item": "Immediate Portion Audit / Vendor Recipe Cost Review Needed"
        })

    # High Discount Alert (>25%)
    high_dis = item_summary[item_summary['Dis%'] > 0.25].sort_values(by='Dis%', ascending=False)
    for _, r in high_dis.iterrows():
        insights_rows.append({
            "Alert Category": "🏷️ High Discount % (>25%)",
            "Brand Name": r['Brand Name'],
            "Category": r['Category'],
            "Item Name": r['Item Name'],
            "Item Variant": r['Item Variant'],
            "Metric Value": f"{round(r['Dis%']*100, 2)}%",
            "MTD Net Sales": round(r['Net_Sales'], 2),
            "Action Item": "Review Campaign Discounts & Aggregator Promo Margins"
        })

    return pd.DataFrame(insights_rows)

# =========================================================
# 6. GENERATE TAB DATAFRAMES
# =========================================================
print("📊 Generating Aggregated Reports & Insights...")

df_overall = build_report_matrix(df_all, ['Brand Name', 'Category', 'Item Name', 'Item Variant'])
df_ftd_mtd = build_ftd_mtd_tab(df_all)
df_perf = build_performance_tab(df_all)
df_insights = build_insights_tab(df_all)
df_category = build_report_matrix(df_all, ['Category'])
df_fb = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Frozen Bottle', case=False, na=False)], ['Category', 'Item Name', 'Item Variant'])
df_madno = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Madno', case=False, na=False)], ['Category', 'Item Name', 'Item Variant'])
df_boba = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Boba Bar', case=False, na=False)], ['Category', 'Item Name', 'Item Variant'])

df_data_tab = df_all[['Month', 'Brand Name', 'Category', 'Item Name', 'Item Variant', 'Platform Group', 'Gross', 'Discounts', 'Net', 'Materials', 'Qty']].copy()

def clean_df(df):
    if df.empty:
        return df
    return df.replace([np.inf, -np.inf], 0).fillna(0)

df_overall = clean_df(df_overall)
df_ftd_mtd = clean_df(df_ftd_mtd)
df_perf = clean_df(df_perf)
df_insights = clean_df(df_insights)
df_category = clean_df(df_category)
df_fb = clean_df(df_fb)
df_madno = clean_df(df_madno)
df_boba = clean_df(df_boba)
df_data_tab = clean_df(df_data_tab)

# =========================================================
# 7. FORMATTING & GOOGLE SHEETS UPDATER
# =========================================================
def apply_sheet_formatting_and_highlights(ws, df):
    if df.empty:
        return

    sheet_id = ws.id
    num_rows = len(df) + 1
    num_cols = len(df.columns)

    requests_list = [
        # Dark Slate Blue Header
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.12, "green": 0.30, "blue": 0.47},
                        "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True, "fontSize": 10},
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        }
    ]

    green_bg = {"red": 0.85, "green": 0.92, "blue": 0.83}
    red_bg = {"red": 0.95, "green": 0.80, "blue": 0.80}

    for col_idx, col_name in enumerate(df.columns):
        col_lower = col_name.lower()
        if "growth" in col_lower:
            pos_bg, neg_bg = (green_bg, red_bg) if ("sales" in col_lower or "overall growth" in col_lower) else (red_bg, green_bg)

            requests_list.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": "0"}]},
                            "format": {"backgroundColor": pos_bg}
                        }
                    }, "index": 0
                }
            })
            requests_list.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                            "format": {"backgroundColor": neg_bg}
                        }
                    }, "index": 1
                }
            })

    spreadsheet.batch_update({"requests": requests_list})

def write_sheet(sheet_name, df):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
            
        ws.clear()
        if not df.empty:
            data = [df.columns.tolist()] + df.values.tolist()
            ws.update(data)
            apply_sheet_formatting_and_highlights(ws, df)
            print(f"✅ Successfully updated & formatted: {sheet_name} ({len(df)} rows)")
        else:
            print(f"⚠️ Tab {sheet_name} is empty.")
    except Exception as e:
        print(f"❌ Error updating tab {sheet_name}: {e}")

# Write all required sheets
write_sheet("Overall", df_overall)
write_sheet("FTD/MTD", df_ftd_mtd)
write_sheet("Performance", df_perf)
write_sheet("Insights", df_insights)
write_sheet("Category Wise ", df_category)
write_sheet("Frozen Bottle", df_fb)
write_sheet("Madno", df_madno)
write_sheet("Boba Bar", df_boba)

# =========================================================
# 8. MORNING EMAIL NOTIFICATION
# =========================================================
def send_morning_email_notification():
    EMAIL_USER = os.environ.get("EMAIL_USER", "mis2@frozenbottle.in")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")

    if not EMAIL_PASS:
        print("❌ EMAIL_PASS environment variable is missing. Skipping email notification.")
        return

    target_recipients = [
        "vivek@frozenbottle.in",
        "faraz@frozenbottle.in",
        "bangaloreterritorymanager@frozenbottle.in"
    ]

    to_header = ", ".join(target_recipients)
    subject = f"📊 Product Performance Report is Ready - {ftd_date_str}"

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333333; line-height: 1.6;">
        <div style="max-width: 600px; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px; background-color: #ffffff;">
          <h2 style="color: #1F4E78; margin-top: 0;">📊 Product Performance Report is Ready</h2>
          <p>Hi Team,</p>
          <p>The daily item-level product performance report for <strong>{ftd_date_str}</strong> has been updated successfully.</p>
          <p>Please visit the tracker below to access full brand analysis, Top/Bottom 10 performance, and actionable insights (Food Cost & Discount Alerts):</p>
          
          <div style="text-align: center; margin: 30px 0;">
            <a href="{SHEET_LINK}" target="_blank" style="background-color: #1F4E78; color: #ffffff; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 14px; display: inline-block;">
              👉 Open Product Performance Tracker
            </a>
          </div>

          <p style="font-size: 12px; color: #777777; border-top: 1px solid #eeeeee; padding-top: 10px;">
            This is an automated notification sent every morning at 9:00 AM IST.
          </p>
        </div>
      </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = to_header

    msg.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, target_recipients, msg.as_string())
        server.quit()
        print(f"📧 Notification Email sent successfully to: {target_recipients}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

send_morning_email_notification()

print("🎉 Complete Daily Performance Report Automation Finished Successfully!")
