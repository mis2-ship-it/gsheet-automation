import os
import json
import time
import jwt
import requests
import pandas as pd
import gspread

from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

print("🚀 Rista Endpoint Explorer Started")

# =====================================================
# 🔐 AUTH
# =====================================================

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm="HS256"
    )

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =====================================================
# 🔐 GOOGLE SHEETS
# =====================================================

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

spreadsheet = client.open_by_key(
    "1AX6ZpBTqY4e90kYmcSRnogo0IAcu1WnMRNrzdBuy1Kk"
)

print("✅ GSheet Connected")

# =====================================================
# 📅 DATE
# =====================================================

today = datetime.now().strftime("%Y-%m-%d")

from_date = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d %H:%M:%S")

to_date = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

# =====================================================
# 📡 ACTIVE BRANCH
# =====================================================

try:

    r = requests.get(
        "https://api.ristaapps.com/v1/branch/list",
        headers=headers(),
        timeout=30
    )

    branch_json = r.json()

    if isinstance(branch_json, dict):
        branches = branch_json.get(
            "data",
            []
        )
    else:
        branches = branch_json

    active_branch = next(
        (
            b["branchCode"]
            for b in branches
            if b.get("status") == "Active"
        ),
        None
    )

    print(
        "✅ Active Branch:",
        active_branch
    )

except Exception as e:

    print(
        "❌ Branch fetch failed:",
        e
    )

    active_branch = None

# =====================================================
# 📌 ENDPOINT LIST (FULL DISCOVERY)
# =====================================================

endpoint_tabs = [

    # ==========================================
    # BRANCH / OUTLET
    # ==========================================
    "branch_list",
    "branch_details",
    "branch_settings",
    "outlet_status",

    # ==========================================
    # SALES
    # ==========================================
    "sales_page",
    "sales_summary",
    "sales_invoice",
    "sales_order",
    "sales_order_details",
    "sales_voided",
    "sales_cancelled",

    # ==========================================
    # ANALYTICS
    # ==========================================
    "analytics_sales_summary",
    "analytics_custom_sales_summary",
    "analytics_discount_transaction",
    "analytics_tax_summary",
    "analytics_payment_summary",
    "analytics_hourly_sales",
    "analytics_order_summary",
    "analytics_item_sales",
    "analytics_category_sales",
    "analytics_channel_sales",

    # ==========================================
    # ORDERS
    # ==========================================
    "order_status",
    "order_list",
    "order_details",
    "order_summary",

    # ==========================================
    # KOT / KITCHEN
    # ==========================================
    "kot_list",
    "kot_summary",
    "kitchen_orders",

    # ==========================================
    # MENU
    # ==========================================
    "menu_list",
    "menu_item",
    "menu_modifier",
    "menu_category",
    "menu_combo",

    # ==========================================
    # INVENTORY
    # ==========================================
    "inventory_stock",
    "inventory_item",
    "inventory_consumption",
    "inventory_stock_adjustment",
    "inventory_purchase",
    "inventory_vendor",
    "inventory_transfer",

    # ==========================================
    # PAYMENTS
    # ==========================================
    "payment_summary",
    "payment_transaction",
    "payment_mode",
    "payment_settlement",

    # ==========================================
    # DISCOUNTS
    # ==========================================
    "discount_transaction",
    "discount_summary",
    "discount_coupon",

    # ==========================================
    # CUSTOMERS
    # ==========================================
    "customer_list",
    "customer_details",
    "customer_feedback",
    "customer_loyalty",

    # ==========================================
    # EMPLOYEE / STAFF
    # ==========================================
    "employee_list",
    "employee_attendance",
    "employee_shift",

    # ==========================================
    # ONLINE CHANNELS
    # ==========================================
    "swiggy_orders",
    "zomato_orders",
    "online_orders",

    # ==========================================
    # TAX / BILLING
    # ==========================================
    "tax_summary",
    "invoice_list",
    "invoice_details",

    # ==========================================
    # REPORTS
    # ==========================================
    "report_daily_sales",
    "report_hourly_sales",
    "report_store_performance",
    "report_item_performance",

    # ==========================================
    # STOCK / PROCUREMENT
    # ==========================================
    "purchase_order",
    "goods_receipt",
    "stock_transfer",
    "vendor_list",

    # ==========================================
    # DELIVERY
    # ==========================================
    "delivery_orders",
    "delivery_status",

    # ==========================================
    # EXPERIMENTAL / UNKNOWN
    # ==========================================
    "dashboard_summary",
    "dashboard_metrics",
    "business_summary"

]

# =====================================================
# 📊 STATUS LOG
# =====================================================

status_logs = []

# =====================================================
# 🔁 LOOP ENDPOINTS
# =====================================================

for tab_name in endpoint_tabs:

    print(f"\n🔍 Processing {tab_name}")

    endpoint = (
        "/v1/" +
        tab_name.replace("_", "/")
    )

    url = (
        "https://api.ristaapps.com"
        + endpoint
    )

    params = {}

    # =============================================
    # SMART PARAMS
    # =============================================

    if "analytics" in tab_name:
        params["day"] = today

    if "sales" in tab_name:
        params.update({
            "day": today,
            "page": 1,
            "pageSize": 10,
            "sort": "desc"
        })

    if any(
        x in tab_name
        for x in [
            "inventory",
            "menu",
            "payment",
            "order",
            "customer"
        ]
    ):
        params["branch"] = active_branch

    if "page" in tab_name:
        params.update({
            "page": 1,
            "pageSize": 10
        })

    params["fromDate"] = from_date
    params["toDate"] = to_date

    try:

        response = requests.get(
            url,
            headers=headers(),
            params=params,
            timeout=30
        )

        if response.status_code != 200:

            raise Exception(
                f"HTTP {response.status_code}"
            )

        js = response.json()

        # =========================================
        # DATA EXTRACTION
        # =========================================

        data = []

        if isinstance(js, dict):

            if "data" in js:

                if isinstance(
                    js["data"],
                    dict
                ):

                    data = (
                        js["data"]
                        .get("rows", [])
                    )

                    if not data:
                        data = [
                            js["data"]
                        ]

                elif isinstance(
                    js["data"],
                    list
                ):
                    data = js["data"]

            else:
                data = [js]

        elif isinstance(js, list):
            data = js

        if not data:

            raise Exception(
                "No data found"
            )

        # =========================================
        # NORMALIZE JSON
        # =========================================

        df = pd.json_normalize(data)

        df = df.head(10)

        # =========================================
        # CREATE TAB IF MISSING
        # =========================================

        try:
            ws = spreadsheet.worksheet(
                tab_name
            )

        except:

            ws = spreadsheet.add_worksheet(
                title=tab_name,
                rows=1000,
                cols=50
            )

        # =========================================
        # REFRESH DATA
        # =========================================

        ws.clear()

        ws.update(
            [
                df.columns.tolist()
            ] +
            df.astype(str)
            .values
            .tolist()
        )

        print(
            f"✅ {tab_name}: "
            f"{len(df)} rows"
        )

        status_logs.append([
            tab_name,
            endpoint,
            len(df),
            "Success",
            ""
        ])

    except Exception as e:

        print(
            f"❌ {tab_name}: {e}"
        )

        status_logs.append([
            tab_name,
            endpoint,
            0,
            "Failed",
            str(e)
        ])

# =====================================================
# 📊 API STATUS TAB
# =====================================================

status_df = pd.DataFrame(
    status_logs,
    columns=[
        "Tab",
        "Endpoint",
        "Rows",
        "Status",
        "Error"
    ]
)

try:

    status_ws = spreadsheet.worksheet(
        "API_Status"
    )

except:

    status_ws = (
        spreadsheet
        .add_worksheet(
            title="API_Status",
            rows=500,
            cols=20
        )
    )

status_ws.clear()

status_ws.update(
    [
        status_df.columns.tolist()
    ] +
    status_df.astype(str)
    .values
    .tolist()
)

print("🎉 Explorer Completed")
