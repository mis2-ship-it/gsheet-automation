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
# 📌 ENDPOINT MAP (FULL)
# =====================================================

endpoint_map = {

    "/v1/branch/list": {},
    "/v1/branch/details": {"branch": "active_branch"},
    "/v1/branch/settings": {"branch": "active_branch"},
    "/v1/outlet/status": {},

    "/v1/sales/page": {
        "day": "today",
        "page": 1,
        "pageSize": 10,
        "sort": "desc"
    },

    "/v1/sales/summary": {"day": "today"},
    "/v1/sales/invoice": {"day": "today"},
    "/v1/sales/order": {"day": "today"},
    "/v1/sales/order/details": {"day": "today"},
    "/v1/sales/voided": {"day": "today"},
    "/v1/sales/cancelled": {"day": "today"},

    "/v1/analytics/sales/summary": {"day": "today"},
    "/v1/analytics/custom/sales/summary": {"day": "today"},
    "/v1/analytics/discount/transaction": {"day": "today"},
    "/v1/analytics/tax/summary": {"day": "today"},
    "/v1/analytics/payment/summary": {"day": "today"},
    "/v1/analytics/hourly/sales": {"day": "today"},
    "/v1/analytics/order/summary": {"day": "today"},
    "/v1/analytics/item/sales": {"day": "today"},
    "/v1/analytics/category/sales": {"day": "today"},
    "/v1/analytics/channel/sales": {"day": "today"},

    "/v1/order/status": {
        "branch": "active_branch",
        "day": "today"
    },

    "/v1/order/list": {
        "branch": "active_branch",
        "page": 1,
        "pageSize": 10
    },

    "/v1/order/details": {
        "branch": "active_branch"
    },

    "/v1/order/summary": {
        "branch": "active_branch",
        "day": "today"
    },

    "/v1/kot/list": {
        "branch": "active_branch",
        "page": 1,
        "pageSize": 10
    },

    "/v1/kot/summary": {
        "branch": "active_branch",
        "day": "today"
    },

    "/v1/kitchen/orders": {
        "branch": "active_branch"
    },

    "/v1/menu/list": {
        "branch": "active_branch"
    },

    "/v1/menu/item": {
        "branch": "active_branch"
    },

    "/v1/menu/modifier": {
        "branch": "active_branch"
    },

    "/v1/menu/category": {
        "branch": "active_branch"
    },

    "/v1/menu/combo": {
        "branch": "active_branch"
    },

    "/v1/inventory/stock": {
        "branch": "active_branch"
    },

    "/v1/inventory/item": {
        "branch": "active_branch"
    },

    "/v1/inventory/consumption": {
        "branch": "active_branch",
        "fromDate": "from_date",
        "toDate": "to_date"
    },

    "/v1/inventory/stock/adjustment": {
        "branch": "active_branch"
    },

    "/v1/inventory/purchase": {
        "branch": "active_branch"
    },

    "/v1/inventory/vendor": {
        "branch": "active_branch"
    },

    "/v1/inventory/transfer": {
        "branch": "active_branch"
    },

    "/v1/payment/summary": {"day": "today"},
    "/v1/payment/transaction": {"day": "today"},
    "/v1/payment/mode": {},
    "/v1/payment/settlement": {"day": "today"},

    "/v1/discount/transaction": {"day": "today"},
    "/v1/discount/summary": {"day": "today"},
    "/v1/discount/coupon": {},

    "/v1/customer/list": {
        "branch": "active_branch",
        "page": 1,
        "pageSize": 10
    },

    "/v1/customer/details": {
        "branch": "active_branch"
    },

    "/v1/customer/feedback": {
        "day": "today"
    },

    "/v1/customer/loyalty": {},

    "/v1/employee/list": {},
    "/v1/employee/attendance": {
        "day": "today"
    },

    "/v1/employee/shift": {
        "day": "today"
    },

    "/v1/swiggy/orders": {
        "day": "today"
    },

    "/v1/zomato/orders": {
        "day": "today"
    },

    "/v1/online/orders": {
        "day": "today"
    },

    "/v1/tax/summary": {
        "day": "today"
    },

    "/v1/invoice/list": {
        "day": "today"
    },

    "/v1/invoice/details": {
        "day": "today"
    },

    "/v1/report/daily/sales": {
        "day": "today"
    },

    "/v1/report/hourly/sales": {
        "day": "today"
    },

    "/v1/report/store/performance": {
        "day": "today"
    },

    "/v1/report/item/performance": {
        "day": "today"
    },

    "/v1/purchase/order": {
        "branch": "active_branch"
    },

    "/v1/goods/receipt": {
        "branch": "active_branch"
    },

    "/v1/stock/transfer": {
        "branch": "active_branch"
    },

    "/v1/vendor/list": {},

    "/v1/delivery/orders": {
        "day": "today"
    },

    "/v1/delivery/status": {},

    "/v1/dashboard/summary": {},
    "/v1/dashboard/metrics": {},
    "/v1/business/summary": {}
}
# =====================================================
# 📊 STATUS LOG
# =====================================================

status_logs = []

# =====================================================
# 🔁 LOOP ENDPOINTS
# =====================================================

for endpoint, params in endpoint_map.items():

    tab_name = (
        endpoint
        .replace("/v1/", "")
        .replace("/", "_")
    )

    print(f"\n🔍 Processing {tab_name}")

    url = (
        "https://api.ristaapps.com"
        + endpoint
    )

    final_params = {}

    for k, v in params.items():

        if v == "today":
            final_params[k] = today

        elif v == "active_branch":
            final_params[k] = active_branch

        elif v == "from_date":
            final_params[k] = from_date

        elif v == "to_date":
            final_params[k] = to_date

        else:
            final_params[k] = v

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
