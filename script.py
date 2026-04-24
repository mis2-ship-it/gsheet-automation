import os
import json
import pandas as pd
import jwt
import time
import requests
import numpy as np
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Script started")

from datetime import datetime
import pytz

ist = pytz.timezone('Asia/Kolkata')
print("Run Time IST:", datetime.now(ist))

# ---------------- AUTH ---------------- #

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# ---------------- GOOGLE AUTH ---------------- #

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# ---------------- FETCH BRANCHES ---------------- #

b_url = "https://api.ristaapps.com/v1/branch/list"
b_resp = requests.get(b_url, headers=headers())
b_df = pd.DataFrame(b_resp.json())

b_df = b_df[b_df["status"] == "Active"]
b_codes = b_df["branchCode"].tolist()

print("Branches:", len(b_codes))

# ---------------- FETCH SALES ---------------- #

s_url = "https://api.ristaapps.com/v1/sales/page"
Sales = []

now = datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

days = [today, last_week]

for day in days:
    for b in b_codes:
        last_key = None

        while True:
            params = {"branch": b, "day": day}
            if last_key:
                params["lastKey"] = last_key

            resp = requests.get(s_url, headers=headers(), params=params)
            js = resp.json()

            data = js.get("data", [])
            if not data:
                break

            df = pd.json_normalize(data)
            Sales.append(df)

            last_key = js.get("lastKey")
            if not last_key:
                break

print("Fetched batches:", len(Sales))

if not Sales:
    print("⚠️ No data fetched yet, skipping this run")
    exit()

s_df = pd.concat(Sales, ignore_index=True)
print("Raw data:", s_df.shape)

# ---------------- TRANSFORM ---------------- #

exploded_df = s_df.explode("items")
items_df = pd.json_normalize(exploded_df["items"]).add_prefix("item_")
exploded_df = exploded_df.drop(columns=["items"])

merged_df = pd.concat(
    [exploded_df.reset_index(drop=True), items_df.reset_index(drop=True)], axis=1
)

exploded_df2 = merged_df.explode("item_discounts")
exploded_df2["item_discounts"] = exploded_df2["item_discounts"].apply(
    lambda x: x if isinstance(x, dict) else {}
)

dis_df = pd.json_normalize(exploded_df2["item_discounts"]).add_prefix("disCode_")

final_df = pd.concat(
    [exploded_df2.reset_index(drop=True), dis_df.reset_index(drop=True)], axis=1
)

print("After explode:", final_df.shape)

# ---------------- SELECT COLUMNS ---------------- #

reqcolumns = final_df[
    [
        "branchName","brandName","invoiceNumber","invoiceDay","invoiceDate",
        "sessionLabel","channel","chargeAmount","status",
        "item_longName","item_shortName","item_variants",
        "item_skuCode","item_categoryName","item_brandName",
        "item_quantity","item_unitPrice","item_discountAmount",
        "item_grossAmount","item_netAmount","item_baseNetAmount",
        "disCode_name"
    ]
]

reqcolumns = reqcolumns.rename(columns={
    "item_skuCode": "SKU Code",
    "item_shortName": "Item Group Name",
    "item_longName": "Item Name",
    "item_categoryName": "Category",
    "item_quantity": "Quantity",
    "item_unitPrice": "Unit Price",
    "item_discountAmount": "Discount Amount",
    "item_grossAmount": "Gross Amount",
    "item_netAmount": "Net Amount"
})

reqcolumns["invoiceDate"] = pd.to_datetime(reqcolumns["invoiceDate"]).dt.tz_localize(None)
reqcolumns["invoiceDay"] = pd.to_datetime(reqcolumns["invoiceDay"]).dt.strftime("%Y-%m-%d")

reqcolumns = reqcolumns.fillna("")

# ---------------- MASTER DATA ---------------- #

sheet = client.open("Item Level Sales").worksheet("Help Sheet")

branch_master = pd.DataFrame(sheet.get("G:M")[1:], columns=sheet.get("G:M")[0])
storetype_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

reqcolumns["Store Type"] = reqcolumns["branchName"].map(storetype_map)
reqcolumns["Region"] = reqcolumns["branchName"].map(region_map)

source_master = pd.DataFrame(sheet.get("D:E")[1:], columns=sheet.get("D:E")[0])
source_map = dict(zip(source_master["Channel"], source_master["Source"]))

reqcolumns["Source"] = reqcolumns["channel"].map(source_map)

# ---------------- FINAL CLEAN ---------------- #

reqcolumns["Hour"] = pd.to_datetime(reqcolumns["invoiceDate"]).dt.hour
reqcolumns["time"] = pd.to_datetime(reqcolumns["invoiceDate"]).dt.strftime("%H:%M")

reqcolumns = reqcolumns.drop_duplicates(
    subset=["invoiceNumber", "SKU Code", "Item Name"]
)

reqcolumns = reqcolumns.fillna("").replace([np.inf, -np.inf], "").astype(str)

print("Final data:", reqcolumns.shape)

# ---------------- UPLOAD ---------------- #

ws = client.open("Item Level Sales").worksheet("Master")

ws.clear()
ws.update(
    [reqcolumns.columns.tolist()] + reqcolumns.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Upload Completed Successfully")
