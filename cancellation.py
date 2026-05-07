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
mapping_ws = spreadsheet.worksheet("Store_Mapping")

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
# 📡 FETCH SALES DATA
# =========================================================
df_list = []

for branch in branches:
    try:

        from_time = (
        datetime.utcnow() - timedelta(minutes=10)
        ).strftime("%Y-%m-%d %H:%M:%S")

        params = {
           "branch": branch,
           "day": today,
           "fromDate": from_time
        }

        r = requests.get(
            "https://api.ristaapps.com/v1/sales",
            headers=headers(),
            params=params,
            timeout=30
        )

        if r.status_code != 200:
            print(f"❌ API Error {branch}: {r.status_code}")
            print(r.text)
            continue

        resp = r.json()

        data = resp.get("data", []) if isinstance(resp, dict) else resp

        if data:
            df_list.append(pd.json_normalize(data))

    except Exception as e:
        print(f"❌ Error {branch}: {e}")

# Combine all
if not df_list:
    print("❌ No data fetched")
    exit()

df = pd.concat(df_list, ignore_index=True)

print("✅ Data fetched:", len(df))

# =========================================================
# 🔁 STANDARDIZE COLUMN
# =========================================================
df.rename(columns={
    "invoiceNo": "invoiceNumber",
    "orderId": "invoiceNumber"
}, inplace=True)

df["invoiceNumber"] = df["invoiceNumber"].astype(str)

# =========================================================
# 🔻 FILTER CANCELLED
# =========================================================
cancel_df = df[
    df["status"].astype(str).str.lower().isin(["voided"])
].copy()

if cancel_df.empty:
    print("✅ No cancellations")
    exit()

print("🚨 Cancellations Found:", len(cancel_df))

# =========================================================
# 🔁 REMOVE DUPLICATES (IMPORTANT FIX)
# =========================================================

try:
    existing_data = raw_ws.get_all_values()

    if len(existing_data) > 1:

        existing_headers = existing_data[0]

        # Find invoice column safely
        if "invoiceNumber" in existing_headers:

            invoice_idx = existing_headers.index("invoiceNumber")

            sent_ids = set()

            for row in existing_data[1:]:

                try:
                    if len(row) > invoice_idx:
                        sent_ids.add(str(row[invoice_idx]).strip())
                except:
                    pass

            # Remove already alerted orders
            cancel_df = cancel_df[
                ~cancel_df["invoiceNumber"].astype(str).isin(sent_ids)
            ]

except Exception as e:
    print("⚠️ Duplicate check skipped:", e)

# Final validation
if cancel_df.empty:
    print("✅ No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================
data = mapping_ws.get_all_values()
headers_map = [h.strip() for h in data[0]]

mapping_df = pd.DataFrame(data[1:], columns=headers_map)

final_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",
    how="left"
)

# Safe channel
if "channel" not in final_df.columns:
    final_df["channel"] = "Unknown"

print("✅ Mapping Ready:", final_df.shape)

# =========================================================
# 📧 EMAIL FUNCTION
# =========================================================
def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
 

    rows_html = ""

    for _, row in store_df.iterrows():
        rows_html += f"""
        <tr>
            <td>{row.get('invoiceNumber','')}</td>
            <td>{row.get('branchName','')}</td>
            <td>{row.get('channel','Unknown')}</td>
            <td>{row.get('createdDate') or row.get('invoiceDate') or ''}</td>
            <td>{row.get('netAmount') or row.get('Net Sales') or ''}</td>
        </tr>
        """

    body = f"""
    <h2>🚨 Cancellation Alert</h2>
    <p><b>Store:</b> {store_df['branchName'].iloc[0]}</p>
    
    <p style="color:red; font-weight:bold;">
    ⚠️ Please check and update the reason for cancellation immediately.
    </p>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Order ID</th>
            <th>Store</th>
            <th>Channel</th>
            <th>Time</th>
            <th>Amount</th>
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
for store, group in final_df.groupby("branchName"):

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

    send_email(",".join(receivers), group)

    print(f"📩 Alert sent for {store} → {receivers}")


# =========================================================
# 📧 SUMMARY EMAIL (ONLY CC)
# =========================================================
def send_summary_email(summary_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CCOPS")

    if not CC_EMAIL:
        print("❌ No CC email configured")
        return

    # Convert dataframe to HTML
    table_html = summary_df.to_html(index=False, border=1)

    body = f"""
    <h2>📊 Store-Level Cancellation Summary</h2>

    <p><b>Channel-wise cancellation count</b></p>

    {table_html}

    <br>
    <p style="color:red;"><b>⚠️ Please review high cancellation stores.</b></p>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = "📊 Cancellation Summary Report"
    msg["From"] = EMAIL_USER
    msg["To"] = CC_EMAIL   # sending only to CC list

    receivers = [e.strip() for e in CC_EMAIL.split(",") if e.strip()]

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, receivers, msg.as_string())
        server.quit()

        print(f"📩 Summary mail sent → {receivers}")

    except Exception as e:
        print(f"❌ Summary email error: {e}")

# =========================================================
# 📊 CHANNEL-WISE SUMMARY
# =========================================================
summary_df = (
    final_df
    .groupby(["branchName", "channel"])
    .size()
    .reset_index(name="Cancel_Count")
)

print(summary_df.head())

# =========================================================
# 📤 SEND SUMMARY MAIL
# =========================================================
send_summary_email(summary_df)

        
# =========================================================
# 📊 SAVE TO SHEET
# =========================================================
final_df["createdAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
final_df["emailSent"] = "YES"
final_df["status_flag"] = "SENT"

clean_df = final_df.replace([np.inf, -np.inf], 0).fillna("").astype(str)

if raw_ws.row_count == 1:
    raw_ws.append_row(clean_df.columns.tolist())

print("✅ Data appended to sheet")
print("🎉 Flow Completed")


