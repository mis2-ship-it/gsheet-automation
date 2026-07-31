import os
import time
import jwt
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# =========================================================
# 🔑 CONFIGURATION & AUTHENTICATION
# =========================================================

BASE_URL = "https://api.ristaapps.com/v1"

# Fetch environment variables directly
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def headers():
    """Returns required HTTP headers for Rista API requests."""
    return {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "x-channel-id": SECRET_KEY
    }

# =========================================================
# 📅 DATE RANGE & BRANCHES SETUP
# =========================================================

today_dt = datetime.now()
last_week_dt = today_dt - timedelta(days=7)

today = today_dt.strftime("%Y-%m-%d")
last_week = last_week_dt.strftime("%Y-%m-%d")

branches = ["BRANCH_001"]

# =========================================================
# 📦 FETCH INVENTORY DATA
# =========================================================

def fetch_inventory_data():
    print("--- Fetching Inventory Data ---")
    
    # 1. Fetch Inventory Transfers (GET with Query Parameters)
    transfer_url = f"{BASE_URL}/inventory/transfer/page"
    transfer_params = {
        "fromDate": last_week,
        "toDate": today,
        "page": 1,
        "pageSize": 50
    }
    
    print(f"Calling (GET): {transfer_url}")
    try:
        r_transfer = requests.get(
            transfer_url, 
            headers=headers(), 
            params=transfer_params, 
            timeout=20
        )
        r_transfer.raise_for_status()
        transfer_data = r_transfer.json()
        print("✅ Inventory Transfers Fetched Successfully")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Warning: Failed for /inventory/transfer/page | Error: {e}")
        transfer_data = {}

    # 2. Fetch Goods Received Note - GRN (GET with Query Parameters)
    grn_url = f"{BASE_URL}/inventory/grn/page"
    grn_params = {
        "fromDate": last_week,
        "toDate": today,
        "page": 1,
        "pageSize": 50
    }
    
    print(f"Calling (GET): {grn_url}")
    try:
        r_grn = requests.get(
            grn_url, 
            headers=headers(), 
            params=grn_params, 
            timeout=20
        )
        r_grn.raise_for_status()
        grn_data = r_grn.json()
        print("✅ GRN Data Fetched Successfully")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Warning: Failed for /inventory/grn/page | Error: {e}")
        grn_data = {}

    # 3. Fetch Item Stock (POST with Body Payload)
    stock_url = f"{BASE_URL}/inventory/item/stock"
    stock_payload = {
        "branchCodes": branches
    }
    
    print(f"Calling (POST): {stock_url}")
    try:
        r_stock = requests.post(
            stock_url, 
            headers=headers(), 
            json=stock_payload, 
            timeout=20
        )
        r_stock.raise_for_status()
        stock_data = r_stock.json()
        print("✅ Item Stock Fetched Successfully")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Warning: Failed for /inventory/item/stock | Error: {e}")
        stock_data = {}

    return transfer_data, grn_data, stock_data

# =========================================================
# 🚀 MAIN EXECUTION
# =========================================================

if __name__ == "__main__":
    print(f"Starting script run for date range: {last_week} to {today}\n")
    
    # Execute inventory call
    transfer_data, grn_data, stock_data = fetch_inventory_data()
    
    # Summary Output
    print("\n--- Summary of Results ---")
    print(f"Transfer Data Keys: {list(transfer_data.keys()) if isinstance(transfer_data, dict) else 'List Response'}")
    print(f"GRN Data Keys: {list(grn_data.keys()) if isinstance(grn_data, dict) else 'List Response'}")
    print(f"Stock Data Keys: {list(stock_data.keys()) if isinstance(stock_data, dict) else 'List Response'}")
