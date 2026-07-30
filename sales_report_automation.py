import os
import json
import time
import jwt
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Starting Item Level Performance Report Script")

# =========================================================
# 1. AUTHENTICATION & ENVIRONMENT
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]
GSHEET_KEY = "1PhVeFoPERJODrGPW68ORAO0kJjToB5ZYMY-yOkGTWDk"

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

# Current Month MTD: 1st to Yesterday
mtd_start = yesterday.replace(day=1)
mtd_end = yesterday

# Previous Month pMTD: 1st to Same Day Last Month
# Handle January transition
if mtd_start.month == 1:
    pmtd_start = mtd_start.replace(year=mtd_start.year - 1, month=12, day=1)
else:
    pmtd_start = mtd_start.replace(month=mtd_start.month - 1, day=1)

# Handle last day of previous month if current month day exceeds previous month days
import calendar
_, max_days_pm = calendar.monthrange(pmtd_start.year, pmtd_start.month)
pmtd_day = min(yesterday.day, max_days_pm)
pmtd_end = pmtd_start.replace(day=pmtd_day)

ftd_date_str = yesterday.strftime("%Y-%m-%d")
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
    help_data = help_ws.get_all_records()
    help_df = pd.DataFrame(help_data)
    
    # Normalize column names
    help_df.columns = help_df.columns.str.strip()
    
    # Identify branch code column
    branch_col = None
    for col in help_df.columns:
        if col.lower().replace(" ", "") in ["branchcode", "storecode", "code"]:
            branch_col = col
            break
            
    if not branch_col:
        branch_col = help_df.columns[0] # Default to first column
        
    valid_branches = help_df[branch_col].dropna().astype(str).str.strip().unique().tolist()
    print(f"✅ Loaded {len(valid_branches)} valid store branch codes from Help Sheet.")
except Exception as e:
    print(f"❌ Error loading Help Sheet: {e}")
    exit(1)

# =========================================================
# 4. RISTA API DATA FETCHING
# =========================================================
sales_url = "https://api.ristaapps.com/v1/sales/page"

def fetch_sales_for_date_range(start_dt, end_dt):
    all_rows = []
    curr_dt = start_dt
    
    while curr_dt <= end_dt:
        day_str = curr_dt.strftime("%Y-%m-%d")
        print(f"📦 Fetching Rista Sales Data for: {day_str}")
        
        for branch in valid_branches:
            params = {"branch": branch, "day": day_str}
            try:
                res = requests.get(sales_url, headers=get_headers(), params=params, timeout=60)
                if res.status_code == 200:
                    data = res.json().get("data", [])
                    for order in data:
                        # Extract order level channel/platform
                        channel = order.get("Channel") or order.get("channel") or "Ownly"
                        
                        # Map channels to Platform Group
                        channel_str = str(channel).lower()
                        if "swiggy" in channel_str:
                            platform_group = "Swiggy"
                        elif "zomato" in channel_str:
                            platform_group = "Zomato"
                        else:
                            platform_group = "Ownly"

                        # Process item array inside order
                        items = order.get("items", order.get("orderItems", [order]))
                        for item in items:
                            brand_name = item.get("brandName") or order.get("brandName") or "Frozen Bottle"
                            category = item.get("item_categoryName") or item.get("categoryName") or "General"
                            item_name = item.get("item_shortName") or item.get("itemName") or "Unknown"
                            
                            qty = float(item.get("item_quantity", 0) or 0)
                            gross = float(item.get("item_grossAmount", 0) or 0)
                            
                            # Net Discount converted to positive
                            discount = abs(float(item.get("item_netDiscountAmount", 0) or 0))
                            net = float(item.get("item_netAmount", 0) or 0)
                            materials = float(item.get("item_itemMaterialCost", 0) or 0)

                            all_rows.append({
                                "Date": day_str,
                                "Month": curr_dt.strftime("%b-%y"),
                                "Period": "MTD" if curr_dt >= mtd_start else "pMTD",
                                "Brand Name": brand_name,
                                "Category": category,
                                "Item Name": item_name,
                                "Platform Group": platform_group,
                                "Gross": gross,
                                "Discounts": discount,
                                "Net": net,
                                "Materials": materials,
                                "Qty": qty
                            })
            except Exception as err:
                print(f"⚠️ Error fetching branch {branch} on {day_str}: {err}")
                
        curr_dt += timedelta(days=1)
        
    return pd.DataFrame(all_rows)

# Fetch MTD & pMTD Data
df_pmtd = fetch_sales_for_date_range(pmtd_start, pmtd_end)
df_mtd = fetch_sales_for_date_range(mtd_start, mtd_end)

df_all = pd.concat([df_pmtd, df_mtd], ignore_index=True)

if df_all.empty:
    print("❌ No data fetched from API. Exiting.")
    exit()

print(f"✅ Total Raw Records Fetched: {len(df_all)}")

# =========================================================
# 5. DATA AGGREGATION BUILDER
# =========================================================
def build_report_matrix(df, group_cols):
    """
    Builds standard comparison table (Sales, Discount %, Food Cost % across channels)
    comparing pMTD vs MTD with Growth %
    """
    # Group by Period, Platform Group, and custom group_cols
    agg_df = df.groupby(['Period', 'Platform Group'] + group_cols).agg(
        Gross=('Gross', 'sum'),
        Discounts=('Discounts', 'sum'),
        Net=('Net', 'sum'),
        Materials=('Materials', 'sum'),
        Qty=('Qty', 'sum')
    ).reset_index()

    # Pivot to create comparisons
    pivot_df = agg_df.pivot_table(
        index=group_cols,
        columns=['Platform Group', 'Period'],
        values=['Net', 'Discounts', 'Gross', 'Materials'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Calculate Overall metrics across channels
    # Flatten columns for easy calculation
    res_rows = []
    
    unique_items = agg_df[group_cols].drop_duplicates()
    
    for _, row_keys in unique_items.iterrows():
        sub = agg_df
        for col in group_cols:
            sub = sub[sub[col] == row_keys[col]]
            
        row_dict = row_keys.to_dict()
        
        # Overall pMTD & MTD
        pmtd_gross = sub[sub['Period'] == 'pMTD']['Gross'].sum()
        mtd_gross = sub[sub['Period'] == 'MTD']['Gross'].sum()
        
        pmtd_disc = sub[sub['Period'] == 'pMTD']['Discounts'].sum()
        mtd_disc = sub[sub['Period'] == 'MTD']['Discounts'].sum()
        
        pmtd_net = sub[sub['Period'] == 'pMTD']['Net'].sum()
        mtd_net = sub[sub['Period'] == 'MTD']['Net'].sum()
        
        pmtd_mat = sub[sub['Period'] == 'pMTD']['Materials'].sum()
        mtd_mat = sub[sub['Period'] == 'MTD']['Materials'].sum()
        
        # Overall Metrics
        row_dict[f'Overall Sales ({pmtd_label})'] = pmtd_net
        row_dict[f'Overall Sales ({mtd_label})'] = mtd_net
        row_dict['Overall Growth %'] = (mtd_net - pmtd_net) / pmtd_net if pmtd_net > 0 else 0
        
        row_dict[f'Dis% ({pmtd_label})'] = pmtd_disc / pmtd_gross if pmtd_gross > 0 else 0
        row_dict[f'Dis% ({mtd_label})'] = mtd_disc / mtd_gross if mtd_gross > 0 else 0
        row_dict['Dis% Growth %'] = row_dict[f'Dis% ({mtd_label})'] - row_dict[f'Dis% ({pmtd_label})']
        
        row_dict[f'Food Cost % ({pmtd_label})'] = pmtd_mat / pmtd_net if pmtd_net > 0 else 0
        row_dict[f'Food Cost % ({mtd_label})'] = mtd_mat / mtd_net if mtd_net > 0 else 0
        row_dict['Food Cost % Growth %'] = row_dict[f'Food Cost % ({mtd_label})'] - row_dict[f'Food Cost % ({pmtd_label})']
        
        # Channel Metrics: In Store (Ownly), Swiggy, Zomato
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
            
            row_dict[f'{prefix} Sales ({pmtd_label})'] = c_pmtd_net
            row_dict[f'{prefix} Sales ({mtd_label})'] = c_mtd_net
            row_dict[f'{prefix} Sales Growth %'] = (c_mtd_net - c_pmtd_net) / c_pmtd_net if c_pmtd_net > 0 else 0
            
            row_dict[f'{prefix} Dis% ({pmtd_label})'] = c_pmtd_disc / c_pmtd_gross if c_pmtd_gross > 0 else 0
            row_dict[f'{prefix} Dis% ({mtd_label})'] = c_mtd_disc / c_mtd_gross if c_mtd_gross > 0 else 0
            row_dict[f'{prefix} Dis% Growth %'] = row_dict[f'{prefix} Dis% ({mtd_label})'] - row_dict[f'{prefix} Dis% ({pmtd_label})']
            
            row_dict[f'{prefix} Food Cost % ({pmtd_label})'] = c_pmtd_mat / c_pmtd_net if c_pmtd_net > 0 else 0
            row_dict[f'{prefix} Food Cost % ({mtd_label})'] = c_mtd_mat / c_mtd_net if c_mtd_net > 0 else 0
            row_dict[f'{prefix} Food Cost % Growth %'] = row_dict[f'{prefix} Food Cost % ({mtd_label})'] - row_dict[f'{prefix} Food Cost % ({pmtd_label})']

        res_rows.append(row_dict)
        
    return pd.DataFrame(res_rows)

# =========================================================
# 6. GENERATE TAB DATAFRAMES
# =========================================================
print("📊 Generating Aggregated Reports...")

# Overall Tab (Category Name + Item Name)
df_overall = build_report_matrix(df_all, ['Category', 'Item Name'])

# Category Wise Tab
df_category = build_report_matrix(df_all, ['Category'])

# Brand Specific Tabs
df_fb = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Frozen Bottle', case=False, na=False)], ['Category', 'Item Name'])
df_madno = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Madno', case=False, na=False)], ['Category', 'Item Name'])
df_boba = build_report_matrix(df_all[df_all['Brand Name'].str.contains('Boba Bar', case=False, na=False)], ['Category', 'Item Name'])

# Data Tab (Raw Granular)
df_data_tab = df_all[['Month', 'Brand Name', 'Category', 'Item Name', 'Platform Group', 'Gross', 'Discounts', 'Net', 'Materials', 'Qty']].copy()

# Clean NaN and infinite values
def clean_df(df):
    if df.empty:
        return df
    df = df.replace([np.inf, -np.inf], 0).fillna(0)
    return df

df_overall = clean_df(df_overall)
df_category = clean_df(df_category)
df_fb = clean_df(df_fb)
df_madno = clean_df(df_madno)
df_boba = clean_df(df_boba)
df_data_tab = clean_df(df_data_tab)

# =========================================================
# 7. WRITE TO GOOGLE SHEET
# =========================================================
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
            print(f"✅ Successfully updated tab: {sheet_name} ({len(df)} rows)")
        else:
            print(f"⚠️ Tab {sheet_name} is empty.")
    except Exception as e:
        print(f"❌ Error updating tab {sheet_name}: {e}")

write_sheet("Overall", df_overall)
write_sheet("Category Wise ", df_category)
write_sheet("Frozen Bottle", df_fb)
write_sheet("Madno", df_madno)
write_sheet("Boba Bar", df_boba)
write_sheet("Data", df_data_tab)

print("🎉 Item Level Performance Report Daily Automation Completed Successfully!")
