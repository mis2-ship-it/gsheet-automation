# =========================================================
# 🔥 IMPORTS
# =========================================================
import os, json, time, jwt, requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
import numpy as np

print("🚀 Cancellation Script Started")

# =========================================================
# 🔐 AUTH
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }
# =========================================================
# 🧠 CANCELLATION GROUPING
# =========================================================
def classify_reason(reason):

    reason = str(reason).lower()

    # Store Closed
    if any(x in reason for x in [
        "closed",
        "restaurant is now closed",
        "store closed"
    ]):
        return "Store Closed"

    # Store Busy / Delay
    elif any(x in reason for x in [
        "running late",
        "busy",
        "delay",
        "preparation"
    ]):
        return "Store Busy"

    # Out of Stock
    elif any(x in reason for x in [
        "out of stock",
        "not available",
        "unavailable",
        "item unavailable"
    ]):
        return "Out of Stock"

    # Customer Cancelled
    elif any(x in reason for x in [
        "customer",
        "cancelled by customer"
    ]):
        return "Customer Cancelled"

    # Payment Issues
    elif any(x in reason for x in [
        "payment",
        "gateway",
        "transaction",
        "declined"
    ]):
        return "Payment Issue"

    else:
        return "Other"

# =========================================================
# 🔐 GOOGLE SHEETS
# =========================================================
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)
spreadsheet = client.open("Cancellation Dashboard")

raw_ws = spreadsheet.worksheet("Cancellation_Tracker")
try:

    alert_ws = spreadsheet.worksheet(
        "Alert_History"
    )

except:

    alert_ws = spreadsheet.add_worksheet(
        title="Alert_History",
        rows=100000,
        cols=20
    )
mapping_ws = spreadsheet.worksheet("Store_Mapping")
reason_ws = spreadsheet.worksheet("Reason_Map")

reason_data = reason_ws.get_all_records()
reason_map_df = pd.DataFrame(reason_data)

print("✅ Reason Map Loaded:", len(reason_map_df))

print("✅ Google Connected")

# =========================================================
# 📅 DATE
# =========================================================
now = datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_business_day(now):
    return (now - timedelta(days=1)).date() if now.hour < 6 else now.date()

today = get_business_day(now).strftime("%Y-%m-%d")
print("📅 Business Day:", today)

# =========================================================
# 📡 FETCH BRANCHES
# =========================================================
b_resp = requests.get(
    "https://api.ristaapps.com/v1/branch/list",
    headers=headers()
)

data = b_resp.json()

if isinstance(data, dict):
    branch_data = data.get("data", [])
else:
    branch_data = data

branches = [
    b.get("branchCode")
    for b in branch_data
    if b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

# =========================================================
# 📡 FETCH SALES DATA (LATEST 5 MINUTES)
# =========================================================
df_list = []

for branch in branches:

    try:

        # =====================================================
        # ⏰ LAST 5 MINUTES WINDOW
        # =====================================================
        to_time = datetime.utcnow()

        from_time = to_time - timedelta(minutes=5)

        from_time_str = from_time.strftime("%Y-%m-%d %H:%M:%S")
        to_time_str = to_time.strftime("%Y-%m-%d %H:%M:%S")

        print(f"🔍 Fetching {branch} | {from_time_str} → {to_time_str}")

        # =====================================================
        # 📡 API CALL
        # =====================================================
        r = requests.get(
        "https://api.ristaapps.com/v1/sales/page",
        headers=headers(),
        params={
            "branch": branch,
            "day": today,
            "fromDate": from_time_str,
            "toDate": to_time_str,
            "page": 1,
            "pageSize": 500,
            "sort": "desc"
        },
        timeout=30
    )

        # =====================================================
        # ❌ STATUS CHECK
        # =====================================================
        if r.status_code != 200:

            print(f"❌ API Error {branch}: {r.status_code}")
            print(r.text)

            continue

        # =====================================================
        # 📦 RESPONSE
        # =====================================================
        resp = r.json()

        data = []

        # CASE 1 → DICT RESPONSE
        if isinstance(resp, dict):

            # /sales/page format
            if "data" in resp:

                if isinstance(resp["data"], dict):

                    data = resp["data"].get("rows", [])

                elif isinstance(resp["data"], list):

                    data = resp["data"]

        # CASE 2 → LIST RESPONSE
        elif isinstance(resp, list):

            data = resp

        # =====================================================
        # 📊 DATAFRAME
        # =====================================================
        if data:

            temp_df = pd.json_normalize(data)

            # Add branchName if missing
            if "branchName" not in temp_df.columns:

                temp_df["branchName"] = branch

            df_list.append(temp_df)

            print(f"✅ {branch} → {len(temp_df)} rows")

        else:

            print(f"⚠️ No data → {branch}")

    except Exception as e:

        print(f"❌ Error {branch}: {e}")

# =========================================================
# 🔗 COMBINE ALL DATA
# =========================================================
if not df_list:

    print("❌ No data fetched")
    exit()

df = pd.concat(df_list, ignore_index=True)

print("✅ Total rows fetched:", len(df))

# =========================================================
# 🔻 FILTER CANCELLED ONLY
# =========================================================
status_col = None

possible_status_cols = [
    "status",
    "orderStatus",
    "invoiceStatus"
]

for col in possible_status_cols:

    if col in df.columns:

        status_col = col
        break

if not status_col:

    print("❌ Status column not found")
    print(df.columns.tolist())
    exit()

cancel_df = df[
    df[status_col]
    .astype(str)
    .str.lower()
    .isin(["voided", "cancel"])
].copy()

print("🚨 Cancellation Found:", len(cancel_df))

# =========================================================
# 🧾 CANCEL REASON EXTRACTION
# =========================================================

if "statusInfo.reason" in cancel_df.columns:

    cancel_df["cancelReason"] = (
        cancel_df["statusInfo.reason"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

else:
    cancel_df["cancelReason"] = ""

# Debug check
print(cancel_df[
    ["invoiceNumber", "cancelReason"]
].head())

# =========================================================
# 🔁 STANDARDIZE INVOICE NUMBER
# =========================================================
invoice_col = None

possible_invoice_cols = [
    "invoiceNumber",
    "invoiceNo",
    "orderId",
    "billNo"
]

for col in possible_invoice_cols:

    if col in cancel_df.columns:

        invoice_col = col
        break

if not invoice_col:

    print("❌ Invoice column not found")
    print(cancel_df.columns.tolist())
    exit()

cancel_df["invoiceNumber"] = (
    cancel_df[invoice_col]
    .astype(str)
    .str.strip()
)

# =========================================================
# 🚫 REMOVE DUPLICATE ALERTS
# =========================================================
try:

    existing = pd.DataFrame(raw_ws.get_all_records())

except:

    existing = pd.DataFrame()

if (
    not existing.empty
    and "invoiceNumber" in existing.columns
):

    existing["invoiceNumber"] = (
        existing["invoiceNumber"]
        .astype(str)
        .str.strip()
    )

    sent_ids = set(
    existing["invoiceNumber"]
    .astype(str)
    .str.strip()
    )

    before_count = len(cancel_df)

    cancel_df = cancel_df[
        ~cancel_df["invoiceNumber"].isin(sent_ids)
    ]

    after_count = len(cancel_df)

    print(f"🚫 Duplicate removed: {before_count - after_count}")

# =========================================================
# ✅ FINAL CHECK
# =========================================================
if cancel_df.empty:

    print("✅ No new cancellations")
    exit()

print("✅ New cancellations:", len(cancel_df))

# =========================================================
# 🧩 STORE MAPPING
# =========================================================
data = mapping_ws.get_all_values()
headers_map = [h.strip() for h in data[0]]

mapping_df = pd.DataFrame(data[1:], columns=headers_map)

# cancel_df is the dataframe after duplicate removal
rdc_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",
    how="left"
)

# =========================================================
# FILTER ONLY COCO STORES
# =========================================================

coco_stores = (
    mapping_df[
        mapping_df["Ownership"]
        .astype(str)
        .str.upper()
        .eq("COCO")
    ]["Store Name"]
    .dropna()
    .unique()
    .tolist()
)

rdc_df = rdc_df[
    rdc_df["branchName"]
    .astype(str)
    .isin(coco_stores)
].copy()

print(
    f"✅ COCO cancellations: {len(rdc_df)}"
)

# =========================================================
# FILTER ONLY COCO STORES
# =========================================================

coco_stores = (
    mapping_df[
        mapping_df["Ownership"]
        .astype(str)
        .str.upper()
        .eq("COCO")
    ]["Store Name"]
    .dropna()
    .unique()
    .tolist()
)

rdc_df = rdc_df[
    rdc_df["branchName"]
    .astype(str)
    .isin(coco_stores)
].copy()

print(
    f"✅ COCO cancellations: {len(rdc_df)}"
)

# =========================================================
# MAP REASON FROM GOOGLE SHEET
# =========================================================

rdc_df["Cancel_Group"] = "Other"
rdc_df["RDC_Flag"] = "No"

for _, r in reason_map_df.iterrows():

    keyword = str(r["Reason (raw, contains)"]).lower().strip()

    if keyword == "":
        continue

    mask = (
        rdc_df["cancelReason"]
        .fillna("")
        .str.lower()
        .str.contains(keyword, na=False)
    )

    rdc_df.loc[mask, "Cancel_Group"] = r["Bucket"]

    if "RDC" in str(r["Notes"]).upper():
        rdc_df.loc[mask, "RDC_Flag"] = "Yes"

print(
    "✅ RDC Orders:",
    len(rdc_df[rdc_df["RDC_Flag"] == "Yes"])
)

# =========================================================
# 🧾 CANCELLATION REASON
# =========================================================

possible_reason_cols = [
    "cancelReason",
    "cancellationReason",
    "voidReason",
    "reason",
    "statusInfo.reason"
]

reason_col = None

for col in possible_reason_cols:
    if col in rdc_df.columns:
        reason_col = col
        break

if reason_col:
    rdc_df["Cancel_Reason"] = rdc_df[reason_col].fillna("Unknown")
else:
    rdc_df["Cancel_Reason"] = "Unknown"

existing_data = alert_ws.get_all_records()

if existing_data:

    existing_df = pd.DataFrame(existing_data)

    alerted_invoices = (
        existing_df["invoiceNumber"]
        .astype(str)
        .unique()
        .tolist()
    )

else:

    alerted_invoices = []

print(existing_df.columns.tolist())



# =========================================================
# 📧 EMAIL FUNCTION
# =========================================================
def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")

    store_name = store_df["branchName"].iloc[0]


    rows_html = ""

    for _, row in store_df.iterrows():
        rows_html += f"""
        <tr>
            <td>{row.get('invoiceNumber','')}</td>
            <td>{row.get('branchName','')}</td>
            <td>{row.get('channel','Unknown')}</td>
            <td>{row.get('createdDate') or row.get('invoiceDate') or ''}</td>
            <td>{row.get('netAmount') or row.get('Net Sales') or ''}</td>
            <td>{row.get('Cancel_Reason','Unknown')}</td>
        </tr>
        """

    # =====================================================
    # 📊 CHANNEL SUMMARY
    # =====================================================
    channel_summary = (
        store_df.groupby("channel")
        .size()
        .reset_index(name="Count")
    )

    channel_html = ""

    for _, row in channel_summary.iterrows():

        channel_html += f"""
        <tr>
            <td>{row['channel']}</td>
            <td>{row['Count']}</td>
        </tr>
        """

    # =====================================================
    # 📊 REASON SUMMARY
    # =====================================================
    reason_summary = (
        store_df.groupby("Cancel_Group")
        .size()
        .reset_index(name="Count")
    )

    reason_html = ""

    for _, row in reason_summary.iterrows():

        reason_html += f"""
        <tr>
            <td>{row['Cancel_Group']}</td>
            <td>{row['Count']}</td>
        </tr>
        """

    body = f"""
    <h2>🚨 Cancellation Alert</h2>
    
    <p><b>Store:</b> {store_name}</p>
    
    <p style="color:red;">
    ⚠ Please check and update the reason for cancellation immediately.
    </p>
    
    <h3>📊 Channel-wise Cancellation Count</h3>
    
    <table border="1" cellpadding="5">
    <tr>
        <th>Channel</th>
        <th>Count</th>
    </tr>
    {channel_html}
    </table>
    
    <br>
    
    <h3>📊 Cancellation Reason Summary</h3>
    
    <table border="1" cellpadding="5">
    <tr>
        <th>Cancellation Type</th>
        <th>Count</th>
    </tr>
    {reason_html}
    </table>
    
    <br>
    
    <h3>📋 Detailed Cancellation Log</h3>
    
    <table border="1" cellpadding="5">
    <tr>
        <th>Order ID</th>
        <th>Store</th>
        <th>Channel</th>
        <th>Time</th>
        <th>Amount</th>
        <th>Reason</th>
    </tr>
    
    {rows_html}
    
    </table>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = f"🚨 Cancellation Alert - {store_df['branchName'].iloc[0]}"
    msg["From"] = EMAIL_USER
    to_list = [e.strip() for e in to_email.split(",") if e.strip()]
    msg["To"] = ", ".join(to_list)
    receivers = to_list.copy()


    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, receivers, msg.as_string())
        server.quit()

        print(f"📩 Mail sent → {to_email}")

    except Exception as e:
        print(f"❌ Email error: {e}")

# =========================================================
# 🚀 SEND ALERTS (TEAM + REGION MANAGER)
# =========================================================
rdc_df = rdc_df[
    rdc_df["RDC_Flag"] == "Yes"
].copy()

for store, group in rdc_df.groupby("branchName"):

    # Team email
    team_email = str(group["Email"].iloc[0]).strip() if "Email" in group.columns else ""

    # Region Manager email
    rm_email = str(group["Region Manager Email"].iloc[0]).strip() if "Region Manager Email" in group.columns else ""

    receivers = []

    # Add team email
    if team_email and team_email.lower() != "nan":
        receivers.append(team_email)

    # Add RM email
    if rm_email and rm_email.lower() != "nan":
        receivers.append(rm_email)

    # Remove duplicates
    receivers = list(set(receivers))

    if not receivers:
        print(f"⚠️ No email mapped for store: {store}")
        continue

    # ==========================================
    # DEBUG EMAIL MAPPING
    # ==========================================
    print("\n========== EMAIL DEBUG ==========")
    print(group[[
        "branchName",
        "Store Name",
        "Email",
        "Region Manager Email"
    ]])
    print("Receivers:", receivers)
    print("=================================\n")

    send_email(",".join(receivers), group)

    print(f"📩 Alert sent for {store} → {receivers}")

    alert_history_df = rdc_df[[
        "invoiceNumber"
    ]].copy()
    
    if len(alert_history_df):
    
        alert_ws.append_rows(
            alert_history_df.values.tolist(),
            value_input_option="USER_ENTERED"
        )
    
    print("✅ Alert history updated")

# =========================================================
# 📊 SUMMARY DATA
# =========================================================

channel_summary = (
    rdc_df.groupby("channel")
    .size()
    .reset_index(name="Count")
    .sort_values("Count", ascending=False)
)

reason_summary = (
    rdc_df.groupby("Cancel_Group")
    .size()
    .reset_index(name="Count")
    .sort_values("Count", ascending=False)
)

store_summary = (
    rdc_df.groupby("branchName")
    .size()
    .reset_index(name="Count")
    .sort_values("Count", ascending=False)
)

critical_summary = (
    rdc_df[
        rdc_df["Cancel_Group"] == "Store Closed"
    ]
    .groupby(["Store Name", "channel"])
    .size()
    .reset_index(name="Count")
    .sort_values("Count", ascending=False)
)

# =========================================================
# 📧 SUMMARY EMAIL
# =========================================================
def send_summary_email(rdc_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CCOPS")

    total_cancel = len(rdc_df)

    if not CC_EMAIL:
        print("❌ No CC email configured")
        return

    # Channel HTML
    channel_html = ""
    for _, row in channel_summary.iterrows():
        channel_html += f"""
        <tr>
            <td>{row['channel']}</td>
            <td>{row['Count']}</td>
        </tr>
        """

    # Reason HTML
    reason_html = ""
    for _, row in reason_summary.iterrows():
        reason_html += f"""
        <tr>
            <td>{row['Cancel_Group']}</td>
            <td>{row['Count']}</td>
        </tr>
        """

# =====================================================
# STORE SUMMARY
# =====================================================

store_summary = (
    rdc_df.groupby("Store Name")
    .size()
    .reset_index(name="Cancel_Count")
    .sort_values(
        "Cancel_Count",
        ascending=False
    )
    .head(10)
)

store_html = ""

for _, row in store_summary.iterrows():

    store_html += f"""
    <tr>
        <td>{row['Store Name']}</td>
        <td>{row['Cancel_Count']}</td>
    </tr>
    """

# Critical HTML
critical_html = ""

for _, row in critical_summary.iterrows():

    critical_html += f"""
    <tr>
        <td>{row['Store Name']}</td>
        <td>{row['channel']}</td>
        <td>{row['Count']}</td>
    </tr>
    """

    body = f"""
    <h2>📊 COCO Cancellation Summary</h2>

    <p><b>Total Cancellations:</b> {total_cancel}</p>
    <p>
        <b>Total Cancellations:</b>
        {total_cancel}
    </p>

    <h3>Channel-wise Cancellation</h3>
    <table border="1" cellpadding="5">
    <tr><th>Channel</th><th>Count</th></tr>
    <tr>
        <th>Channel</th>
        <th>Count</th>
    </tr>
    {channel_html}
    </table>

    <br>

    <h3>Reason-wise Summary</h3>
    <table border="1" cellpadding="5">
    <tr><th>Reason Group</th><th>Count</th></tr>
    <tr>
        <th>Reason Group</th>
        <th>Count</th>
    </tr>
    {reason_html}
    </table>

    <br>

    <h3>🔴 Store Closed (Critical)</h3>
    <table border="1" cellpadding="5">
    <tr>
        <th>Store</th>
        <th>Channel</th>
        <th>Count</th>
    </tr>
    {critical_html}
    </table>

    <br>

    <h3>🏪 Top Impacted Stores</h3>
    <h3>🏪 Store-wise Cancellation</h3>
    <table border="1" cellpadding="5">
    <tr>
        <th>Store</th>
        <th>Count</th>
    </tr>
    {store_html}
    </table>
    """

    msg = MIMEText(body, "html")

    msg["Subject"] = (
    f"🚨 Cancellation Summary | {today}"
    )

    msg["From"] = EMAIL_USER
    msg["To"] = CC_EMAIL

    receivers = [
        e.strip()
        for e in CC_EMAIL.split(",")
        if e.strip()
    ]


    server = smtplib.SMTP(
        "smtp.gmail.com",
        587
    )

    server.starttls()
    server.login(
        EMAIL_USER,
        EMAIL_PASS
    )

    server.sendmail(
        EMAIL_USER,
        receivers,
        msg.as_string()
    )

    server.quit()

    print("📩 Summary email sent")


# =========================================================
# 📤 SEND SUMMARY MAIL
# =========================================================

send_summary_email(rdc_df)


# =========================================================
# 📊 SAVE ALERT HISTORY
# =========================================================

rdc_df["createdAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
rdc_df["emailSent"] = "YES"

# Only tracking columns
tracking_df = rdc_df[[
    "invoiceNumber",
    "branchName",
    "channel",
    "Cancel_Reason",
    "createdAt"
]].copy()

tracking_df = (
    tracking_df
    .replace([np.inf, -np.inf], 0)
    .fillna("")
    .astype(str)
)

# =========================================================
# 🧾 ADD HEADER IF SHEET EMPTY
# =========================================================

existing_rows = raw_ws.get_all_values()

if len(existing_rows) == 0:
    raw_ws.append_row(tracking_df.columns.tolist())

# =========================================================
# ➕ APPEND DATA
# =========================================================

raw_ws.append_rows(
    tracking_df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Tracking data saved")
print("🎉 Flow Completed")
