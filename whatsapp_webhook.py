from flask import Flask, request, jsonify
import os
import requests
import json
import gspread

from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials


# =========================================================
# 🚀 FLASK APP
# =========================================================

app = Flask(__name__)


# =========================================================
# 🔐 ENVIRONMENT VARIABLES
# =========================================================

VERIFY_TOKEN = os.environ.get(
    "WHATSAPP_VERIFY_TOKEN"
)

PHONE_NUMBER_ID = os.environ.get(
    "WHATSAPP_PHONE_NUMBER_ID"
)

ACCESS_TOKEN = os.environ.get(
    "WHATSAPP_ACCESS_TOKEN"
)

WHATSAPP_DATA_SECRET = os.environ.get(
    "WHATSAPP_DATA_SECRET"
)

WHATSAPP_WEBHOOK_DATA_URL = os.environ.get(
    "WHATSAPP_WEBHOOK_DATA_URL"
)

# =========================================================
# LIVE SALES BACKEND SNAPSHOT
# =========================================================

LIVE_SALES_DATA = {}

print(
    "WHATSAPP_DATA_SECRET exists :",
    bool(WHATSAPP_DATA_SECRET)
)

GOOGLE_CREDENTIALS = os.environ.get(
    "GOOGLE_CREDENTIALS"
)

GRAPH_API_VERSION = "v23.0"


# =========================================================
# 📊 GOOGLE SHEET
# =========================================================

SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"
)


spreadsheet = None


def connect_google_sheet():

    global spreadsheet

    print("=" * 60)
    print("📊 CONNECTING GOOGLE SHEET")
    print("=" * 60)

    if not GOOGLE_CREDENTIALS:

        print(
            "❌ GOOGLE_CREDENTIALS is missing"
        )

        return False

    try:

        creds = (
            Credentials
            .from_service_account_info(
                json.loads(
                    GOOGLE_CREDENTIALS
                ),
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
        )

        client = gspread.authorize(
            creds
        )

        spreadsheet = (
            client.open_by_url(
                SHEET_URL
            )
        )

        print(
            "✅ Google Sheet connected"
        )

        return True

    except Exception as e:

        print(
            "❌ Google Sheet connection error:",
            str(e)
        )

        spreadsheet = None

        return False


# Connect when application starts
connect_google_sheet()


# =========================================================
# ⏰ BUSINESS DATE
# =========================================================

def get_ist_now():

    return (
        datetime.utcnow()
        + timedelta(
            hours=5,
            minutes=30
        )
    )


def get_business_day():

    now = get_ist_now()

    # Same logic as rista_live.py
    if now.hour < 6:

        return (
            now.date()
            - timedelta(days=1)
        )

    return now.date()


business_day = get_business_day()


# =========================================================
# 🔄 REFRESH BUSINESS DATE
# =========================================================

def refresh_business_day():

    global business_day

    business_day = get_business_day()

    print(
        "📅 Business Day:",
        business_day
    )

    return business_day


# =========================================================
# 🏠 HOME
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def home():

    return (
        "AI MIS WhatsApp Webhook is running",
        200
    )

# =========================================================
# RISTA LIVE → WHATSAPP DATA RECEIVER
# =========================================================

@app.route("/update-sales-data", methods=["POST"])
def update_sales_data():

    global LIVE_SALES_DATA

    print("=" * 60)
    print("📥 RISTA LIVE SALES DATA RECEIVED")
    print("=" * 60)

    # -----------------------------------------------------
    # SECURITY CHECK
    # -----------------------------------------------------

    incoming_secret = request.headers.get(
        "X-WhatsApp-Data-Secret"
    )

    if not WHATSAPP_DATA_SECRET:

        print(
            "❌ WHATSAPP_DATA_SECRET is not configured"
        )

        return jsonify({
            "success": False,
            "error": "Server secret not configured"
        }), 500

    if incoming_secret != WHATSAPP_DATA_SECRET:

        print(
            "❌ Invalid WhatsApp data secret"
        )

        return jsonify({
            "success": False,
            "error": "Unauthorized"
        }), 401

    # -----------------------------------------------------
    # READ JSON
    # -----------------------------------------------------

    data = request.get_json(
        silent=True
    )

    if not data:

        print(
            "❌ Empty sales data received"
        )

        return jsonify({
            "success": False,
            "error": "Empty JSON payload"
        }), 400

    # -----------------------------------------------------
    # STORE LATEST DATA
    # -----------------------------------------------------

    LIVE_SALES_DATA = data

    print(
        "✅ WhatsApp sales snapshot updated"
    )

    print(
        "Report Date:",
        data.get("date")
    )

    print(
        "Report Time:",
        data.get("report_time")
    )

    print(
        "Available sections:",
        list(data.keys())
    )

    print("=" * 60)

    return jsonify({
        "success": True,
        "message": "Sales data updated successfully"
    }), 200

# =========================================================
# DEBUG — CHECK CURRENT SALES SNAPSHOT
# =========================================================

@app.route("/sales-data", methods=["GET"])
def sales_data():

    if not LIVE_SALES_DATA:

        return jsonify({
            "success": False,
            "message": "No sales data available"
        }), 404

    return jsonify({
        "success": True,
        "data": LIVE_SALES_DATA
    }), 200


# =========================================================
# 🔐 META WEBHOOK VERIFICATION
# =========================================================

@app.route(
    "/webhook",
    methods=["GET"]
)
def verify_webhook():

    mode = request.args.get(
        "hub.mode"
    )

    token = request.args.get(
        "hub.verify_token"
    )

    challenge = request.args.get(
        "hub.challenge"
    )

    print("=" * 60)
    print(
        "🔐 META WEBHOOK VERIFICATION"
    )

    print(
        "Mode:",
        mode
    )

    print(
        "Token OK:",
        token == VERIFY_TOKEN
    )

    print("=" * 60)

    if (
        mode == "subscribe"
        and token == VERIFY_TOKEN
    ):

        print(
            "✅ META WEBHOOK VERIFIED"
        )

        return challenge, 200

    print(
        "❌ WEBHOOK VERIFICATION FAILED"
    )

    return (
        "Forbidden",
        403
    )


# =========================================================
# 📤 SEND WHATSAPP TEXT
# =========================================================

def send_whatsapp_message(
    recipient,
    message
):

    if not PHONE_NUMBER_ID:

        print(
            "❌ WHATSAPP_PHONE_NUMBER_ID missing"
        )

        return False

    if not ACCESS_TOKEN:

        print(
            "❌ WHATSAPP_ACCESS_TOKEN missing"
        )

        return False

    url = (
        f"https://graph.facebook.com/"
        f"{GRAPH_API_VERSION}/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {

        "Authorization":
            f"Bearer {ACCESS_TOKEN}",

        "Content-Type":
            "application/json"
    }

    payload = {

        "messaging_product":
            "whatsapp",

        "to":
            recipient,

        "type":
            "text",

        "text": {

            "preview_url":
                False,

            "body":
                message
        }
    }

    print("=" * 60)
    print(
        "📤 SENDING WHATSAPP MESSAGE"
    )

    print(
        "To:",
        recipient
    )

    print(
        "Message:"
    )

    print(message)

    print("=" * 60)

    try:

        response = requests.post(

            url,

            headers=headers,

            json=payload,

            timeout=30
        )

        print(
            "Meta Status:",
            response.status_code
        )

        print(
            "Meta Response:",
            response.text
        )

        if response.ok:

            print(
                "✅ WhatsApp message sent"
            )

            return True

        print(
            "❌ WhatsApp message failed"
        )

        return False

    except Exception as e:

        print(
            "❌ WhatsApp API error:",
            str(e)
        )

        return False


# =========================================================
# 📊 GOOGLE SHEET READER
# =========================================================

def get_sheet_records(
    sheet_name
):

    global spreadsheet

    try:

        if spreadsheet is None:

            if not connect_google_sheet():

                return []

        ws = (
            spreadsheet
            .worksheet(sheet_name)
        )

        records = (
            ws.get_all_records()
        )

        print(
            f"✅ Read {sheet_name}: "
            f"{len(records)} rows"
        )

        return records

    except Exception as e:

        print(
            f"❌ Error reading "
            f"{sheet_name}:",
            str(e)
        )

        # Try reconnect once

        if connect_google_sheet():

            try:

                ws = (
                    spreadsheet
                    .worksheet(
                        sheet_name
                    )
                )

                records = (
                    ws.get_all_records()
                )

                print(
                    f"✅ Retry successful: "
                    f"{sheet_name}"
                )

                return records

            except Exception as retry_error:

                print(
                    "❌ Retry failed:",
                    str(retry_error)
                )

        return []


# =========================================================
# 🔢 SAFE FLOAT
# =========================================================

def safe_float(value):

    if value is None:

        return 0.0

    try:

        text = str(value).strip()

        if not text:

            return 0.0

        text = (
            text
            .replace(
                "₹",
                ""
            )
            .replace(
                ",",
                ""
            )
            .replace(
                "%",
                ""
            )
            .replace(
                "L",
                ""
            )
            .strip()
        )

        return float(text)

    except Exception:

        return 0.0


# =========================================================
# 📊 READ OVERALL
# =========================================================

def get_overall():

    records = get_sheet_records(
        "Overall"
    )

    if not records:

        raise Exception(
            "Overall sheet returned no data"
        )

    return records


# =========================================================
# 📊 FIND OVERALL PARAMETER
# =========================================================

def get_overall_value(
    records,
    parameter,
    column
):

    for row in records:

        row_parameter = str(
            row.get(
                "Parameters",
                ""
            )
        ).strip().lower()

        if (
            row_parameter
            == parameter.lower()
        ):

            return safe_float(
                row.get(
                    column,
                    0
                )
            )

    return 0.0


# =========================================================
# 📊 FTD SALES DATA
# =========================================================

def get_ftd_sales():

    if not LIVE_SALES_DATA:

        raise Exception(
            "Live sales backend data is not available"
        )

    overall_data = LIVE_SALES_DATA.get(
        "overall",
        {}
    )

    brands = LIVE_SALES_DATA.get(
        "brands",
        {}
    )

    return {

        "net":
            float(
                overall_data.get(
                    "net",
                    0
                )
            ),

        "txn":
            float(
                overall_data.get(
                    "txn",
                    0
                )
            ),

        "aov":
            float(
                overall_data.get(
                    "aov",
                    0
                )
            ),

        "discount":
            float(
                overall_data.get(
                    "discount",
                    0
                )
            ),

        "frozen_bottle":
            float(
                brands.get(
                    "Frozen Bottle",
                    {}
                ).get(
                    "today",
                    0
                )
            ),

        "madno":
            float(
                brands.get(
                    "Madno",
                    {}
                ).get(
                    "today",
                    0
                )
            ),

        "boba_bar":
            float(
                brands.get(
                    "Boba Bar",
                    {}
                ).get(
                    "today",
                    0
                )
            )
    }

# =========================================================
# 📊 SALES VS LAST WEEK DATA
# =========================================================

def get_sales_vs_lw():

    if not LIVE_SALES_DATA:

        raise Exception(
            "Live sales backend data is not available"
        )

    overall_data = LIVE_SALES_DATA.get(
        "overall",
        {}
    )

    return {

        "today":
            float(
                overall_data.get(
                    "net",
                    0
                )
            ),

        "last_week":
            float(
                overall_data.get(
                    "lw_net",
                    0
                )
            ),

        "growth":
            float(
                overall_data.get(
                    "lw_growth",
                    0
                )
            )
    }


# =========================================================
# 🏪 BRAND DATA
# =========================================================

def get_brand_performance():

    if not LIVE_SALES_DATA:

        raise Exception(
            "Live sales backend data is not available"
        )

    return LIVE_SALES_DATA.get(
        "brands",
        {}
    )


# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):

    try:

        print("=" * 60)
        print(
            "📊 FTD SALES REQUEST"
        )
        print("=" * 60)

        sales = get_ftd_sales()

        net = sales[
            "net"
        ]

        txn = sales[
            "txn"
        ]

        aov = sales[
            "aov"
        ]

        discount = sales[
            "discount"
        ]

        today_date = (
            refresh_business_day()
            .strftime(
                "%d-%b-%y"
            )
        )

        reply = (

            "📊 *AI MIS | FTD SALES*\n"
            f"{today_date}\n\n"

            f"💰 Net Revenue: "
            f"₹{net / 100000:.2f}L\n"

            f"🧾 Transactions: "
            f"{int(round(txn)):,}\n"

            f"🧺 AOV: "
            f"₹{int(round(aov)):,}\n"

            f"📉 Discount: "
            f"{discount:.1f}%"
        )

        print(reply)

        send_whatsapp_message(
            sender,
            reply
        )

    except Exception as e:

        print(
            "❌ FTD SALES ERROR:",
            str(e)
        )

        send_whatsapp_message(
            sender,
            "❌ Error while generating "
            "FTD Sales report.\n\n"
            f"Debug: {str(e)}"
        )


# =========================================================
# 📈 SALES VS LAST WEEK RESPONSE
# =========================================================

def send_sales_vs_lw(sender):

    try:

        print("=" * 60)
        print(
            "📈 SALES VS LAST WEEK REQUEST"
        )
        print("=" * 60)

        sales = get_sales_vs_lw()

        today_net = sales[
            "today_net"
        ]

        lw_net = sales[
            "lw_net"
        ]

        growth = sales[
            "growth"
        ]

        today_txn = sales[
            "today_txn"
        ]

        lw_txn = sales[
            "lw_txn"
        ]

        today_aov = sales[
            "today_aov"
        ]

        lw_aov = sales[
            "lw_aov"
        ]

        today_discount = sales[
            "today_discount"
        ]

        lw_discount = sales[
            "lw_discount"
        ]

        if growth > 0:

            growth_icon = "📈"

        elif growth < 0:

            growth_icon = "🔻"

        else:

            growth_icon = "➡️"

        today_date = (
            refresh_business_day()
            .strftime(
                "%d-%b-%y"
            )
        )

        reply = (

            "📊 *AI MIS | SALES VS LW*\n"
            f"{today_date}\n\n"

            "💰 *NET REVENUE*\n"

            f"🟢 Today: "
            f"₹{today_net / 100000:.2f}L\n"

            f"🔵 Last Week: "
            f"₹{lw_net / 100000:.2f}L\n"

            f"{growth_icon} Growth: "
            f"{growth:+.1f}%\n\n"

            "🧾 *TRANSACTIONS*\n"

            f"🟢 Today: "
            f"{int(round(today_txn)):,}\n"

            f"🔵 Last Week: "
            f"{int(round(lw_txn)):,}\n\n"

            "🧺 *AOV*\n"

            f"🟢 Today: "
            f"₹{int(round(today_aov)):,}\n"

            f"🔵 Last Week: "
            f"₹{int(round(lw_aov)):,}\n\n"

            "📉 *DISCOUNT*\n"

            f"🟢 Today: "
            f"{today_discount:.1f}%\n"

            f"🔵 Last Week: "
            f"{lw_discount:.1f}%"
        )

        print(reply)

        send_whatsapp_message(
            sender,
            reply
        )

    except Exception as e:

        print(
            "❌ SALES VS LW ERROR:",
            str(e)
        )

        send_whatsapp_message(
            sender,
            "❌ Error while generating "
            "Sales vs Last Week report.\n\n"
            f"Debug: {str(e)}"
        )


# =========================================================
# 🏪 BRAND PERFORMANCE RESPONSE
# =========================================================

def send_brand_performance(sender):

    try:

        print("=" * 60)
        print(
            "🏪 BRAND PERFORMANCE REQUEST"
        )
        print("=" * 60)

        brands = get_brand_data()

        if not brands:

            raise Exception(
                "No brand data available"
            )

        today_date = (
            refresh_business_day()
            .strftime(
                "%d-%b-%y"
            )
        )

        # ---------------------------------------------
        # TOTAL TODAY BRAND REVENUE
        # ---------------------------------------------

        total_brand = sum(
            item[
                "today_rev"
            ]
            for item in brands
        )

        reply_lines = [

            "🏪 *AI MIS | BRAND PERFORMANCE*",
            today_date,
            ""
        ]

        # ---------------------------------------------
        # BRAND DETAILS
        # ---------------------------------------------

        for item in brands:

            brand = item[
                "brand"
            ]

            revenue = item[
                "today_rev"
            ]

            growth = item[
                "growth"
            ]

            discount = item[
                "today_discount"
            ]

            contribution = (

                revenue
                / max(
                    total_brand,
                    1
                )
            ) * 100

            if (
                growth > 0
            ):

                growth_icon = "📈"

            elif (
                growth < 0
            ):

                growth_icon = "🔻"

            else:

                growth_icon = "➡️"

            if brand.lower() == "frozen bottle":

                icon = "🍶"

            elif brand.lower() == "madno":

                icon = "🥤"

            elif brand.lower() == "boba bar":

                icon = "🧋"

            elif brand.lower() == "lubov":

                icon = "🍨"

            else:

                icon = "🏪"

            reply_lines.extend([

                f"{icon} *{brand}*",

                f"💰 Revenue: "
                f"₹{revenue / 100000:.2f}L",

                f"📊 Contribution: "
                f"{contribution:.0f}%",

                f"{growth_icon} Growth vs LW: "
                f"{growth:+.1f}%",

                f"📉 Discount: "
                f"{discount:.1f}%",

                ""
            ])

        reply = "\n".join(
            reply_lines
        ).strip()

        print(reply)

        send_whatsapp_message(
            sender,
            reply
        )

    except Exception as e:

        print(
            "❌ BRAND PERFORMANCE ERROR:",
            str(e)
        )

        send_whatsapp_message(
            sender,
            "❌ Error while generating "
            "Brand Performance report.\n\n"
            f"Debug: {str(e)}"
        )


# =========================================================
# 📅 YESTERDAY SALES
# =========================================================
#
# IMPORTANT:
# Your current rista_live.py does NOT push a separate
# Yesterday sheet.
#
# So this command is kept as a clear message until
# Yesterday data is added to rista_live.py.
#
# We will add the Yesterday sheet in the next step.
# =========================================================

def send_yesterday_sales(sender):

    print("=" * 60)
    print(
        "📅 YESTERDAY SALES REQUEST"
    )
    print("=" * 60)

    yesterday = (
        refresh_business_day()
        - timedelta(days=1)
    )

    message = (

        "📅 *AI MIS | YESTERDAY SALES*\n"
        f"{yesterday.strftime('%d-%b-%y')}\n\n"

        "⚠️ Yesterday data is not yet "
        "available in the WhatsApp data source.\n\n"

        "The current rista_live.py pushes:\n"
        "• Overall\n"
        "• Source Group\n"
        "• Region\n"
        "• Brand\n"
        "• Session\n"
        "• Top_Stores\n"
        "• Bottom_Stores\n"
        "• Hourly\n\n"

        "We need to add a Yesterday output "
        "to rista_live.py."
    )

    send_whatsapp_message(
        sender,
        message
    )


# =========================================================
# 🧠 PROCESS INCOMING MESSAGE
# =========================================================

def process_message(
    sender,
    message_text
):

    message = " ".join(
        message_text
        .strip()
        .lower()
        .split()
    )

    print("=" * 60)

    print(
        "🧠 PROCESSING MESSAGE"
    )

    print(
        "Sender:",
        sender
    )

    print(
        "Original:",
        message_text
    )

    print(
        "Normalized:",
        message
    )

    print("=" * 60)


    # =====================================================
    # 👋 GREETING
    # =====================================================

    if message in [

        "hi",
        "hello",
        "hey",
        "hii",
        "hiii",
        "good morning",
        "good afternoon",
        "good evening"

    ]:

        reply = (

            "👋 Hi! Welcome to "
            "AI MIS WhatsApp.\n\n"

            "You can ask:\n\n"

            "📊 *SALES*\n"

            "• sales today\n"
            "• yesterday sales\n"
            "• sales vs last week\n\n"

            "🏪 *PERFORMANCE*\n"

            "• brand sales\n\n"

            "❓ help"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return


    # =====================================================
    # ❓ HELP
    # =====================================================

    if message == "help":

        reply = (

            "🤖 *AI MIS WhatsApp*\n\n"

            "Available commands:\n\n"

            "📊 *SALES*\n"

            "• sales today\n"
            "• yesterday sales\n"
            "• sales vs last week\n\n"

            "🏪 *PERFORMANCE*\n"

            "• brand sales\n"
            "• brand performance\n\n"

            "❓ help"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return


    # =====================================================
    # 📅 YESTERDAY SALES
    # =====================================================

    yesterday_keywords = [

        "yesterday",
        "yesterday sales",
        "sales yesterday",
        "yesterday's sales",
        "yesterdays sales",
        "yesterday sale",
        "sales for yesterday",
        "what was yesterday sales",
        "what was yesterday's sales",
        "how was yesterday sales",
        "how was yesterday's sales"

    ]

    if message in yesterday_keywords:

        print(
            "📅 YESTERDAY SALES "
            "COMMAND DETECTED"
        )

        send_yesterday_sales(
            sender
        )

        return


    # =====================================================
    # NATURAL YESTERDAY
    # =====================================================

    if (
        "sales" in message
        and "yesterday" in message
    ):

        print(
            "📅 NATURAL YESTERDAY "
            "SALES QUESTION DETECTED"
        )

        send_yesterday_sales(
            sender
        )

        return


    # =====================================================
    # 📈 SALES VS LAST WEEK
    # =====================================================

    sales_vs_lw_keywords = [

        "sales vs last week",
        "sales versus last week",
        "sales last week",
        "last week sales",
        "lw sales",
        "sales lw",
        "sales vs lw",
        "compare sales last week",
        "compare sales with last week",
        "how are sales vs last week",
        "how is sales vs last week",
        "how was sales vs last week"

    ]

    if message in sales_vs_lw_keywords:

        print(
            "📈 SALES VS LW "
            "COMMAND DETECTED"
        )

        send_sales_vs_lw(
            sender
        )

        return


    # =====================================================
    # NATURAL SALES VS LW
    # =====================================================

    if (
        "sales" in message
        and (
            "last week" in message
            or "lw" in message
        )
    ):

        print(
            "📈 NATURAL SALES VS LW "
            "QUESTION DETECTED"
        )

        send_sales_vs_lw(
            sender
        )

        return


    # =====================================================
    # 🏪 BRAND PERFORMANCE
    # =====================================================

    brand_keywords = [

        "brand",
        "brand sales",
        "brand performance",
        "sales by brand",
        "brand wise sales",
        "brand wise performance",
        "brand performance today"

    ]

    if message in brand_keywords:

        print(
            "🏪 BRAND PERFORMANCE "
            "COMMAND DETECTED"
        )

        send_brand_performance(
            sender
        )

        return


    # =====================================================
    # NATURAL BRAND QUESTIONS
    # =====================================================

    if (
        "brand" in message
        and (
            "sales" in message
            or "performance" in message
        )
    ):

        print(
            "🏪 NATURAL BRAND "
            "PERFORMANCE QUESTION"
        )

        send_brand_performance(
            sender
        )

        return


    # =====================================================
    # 📊 FTD SALES
    # =====================================================

    sales_keywords = [

        "sales",
        "sales today",
        "today sales",
        "today's sales",
        "todays sales",
        "sales for today",
        "today's sale",
        "todays sale",
        "ftd",
        "ftd sales",
        "how was sales today",
        "how was the sales today",
        "how are sales today",
        "how are the sales today",
        "what is today's sales",
        "what is todays sales",
        "what are today's sales",
        "what are todays sales"

    ]

    if message in sales_keywords:

        print(
            "📊 FTD SALES "
            "COMMAND DETECTED"
        )

        send_ftd_sales(
            sender
        )

        return


    # =====================================================
    # NATURAL FTD SALES
    # =====================================================

    if (
        "sales" in message
        and "today" in message
    ):

        print(
            "📊 NATURAL FTD SALES "
            "QUESTION DETECTED"
        )

        send_ftd_sales(
            sender
        )

        return


    # =====================================================
    # ❓ UNKNOWN COMMAND
    # =====================================================

    print(
        "❓ UNKNOWN COMMAND:",
        message
    )

    reply = (

        "🤖 AI MIS received your message:\n\n"

        f"\"{message_text}\"\n\n"

        "Type *help* to see "
        "available commands."
    )

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 📩 META WEBHOOK RECEIVER
# =========================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    data = request.get_json(
        silent=True
    )

    print("=" * 60)

    print(
        "📩 WHATSAPP WEBHOOK RECEIVED"
    )

    print("=" * 60)

    print(
        json.dumps(
            data,
            indent=2,
            ensure_ascii=False
        )
    )

    print("=" * 60)


    if not data:

        print(
            "⚠️ Empty webhook payload"
        )

        return (
            "EVENT_RECEIVED",
            200
        )


    if (
        data.get("object")
        != "whatsapp_business_account"
    ):

        print(
            "⚠️ Not WhatsApp Business Account"
        )

        return (
            "EVENT_RECEIVED",
            200
        )


    # =====================================================
    # PROCESS ENTRIES
    # =====================================================

    for entry in data.get(
        "entry",
        []
    ):

        for change in entry.get(
            "changes",
            []
        ):

            field = change.get(
                "field"
            )

            value = change.get(
                "value",
                {}
            )

            print(
                "Webhook field:",
                field
            )


            if field != "messages":

                print(
                    "ℹ️ Other event:",
                    field
                )

                continue


            messages = value.get(
                "messages",
                []
            )

            print(
                "Number of messages:",
                len(messages)
            )


            for incoming_message in messages:

                message_type = (
                    incoming_message.get(
                        "type"
                    )
                )

                sender = (
                    incoming_message.get(
                        "from"
                    )
                )


                print(
                    "Message type:",
                    message_type
                )

                print(
                    "Sender:",
                    sender
                )


                # =========================================
                # TEXT
                # =========================================

                if message_type == "text":

                    text_data = (
                        incoming_message.get(
                            "text",
                            {}
                        )
                    )

                    message_text = (
                        text_data.get(
                            "body",
                            ""
                        )
                    )

                    print(
                        "💬 Incoming text:",
                        message_text
                    )


                    if (
                        sender
                        and message_text
                    ):

                        try:

                            process_message(
                                sender,
                                message_text
                            )

                            print(
                                "✅ process_message "
                                "completed"
                            )

                        except Exception as e:

                            print(
                                "❌ process_message "
                                "ERROR:",
                                str(e)
                            )

                            send_whatsapp_message(
                                sender,
                                "❌ AI MIS encountered "
                                "an error while "
                                "processing your request."
                            )


                # =========================================
                # NON TEXT
                # =========================================

                else:

                    print(
                        "⚠️ Non-text message:",
                        message_type
                    )

                    if sender:

                        send_whatsapp_message(
                            sender,

                            "🤖 AI MIS currently "
                            "supports text messages only."
                        )


    return (
        "EVENT_RECEIVED",
        200
    )


# =========================================================
# 📤 TEST SEND
# =========================================================

@app.route(
    "/test-send",
    methods=["GET"]
)
def test_send():

    recipients = [

        "919750820509",

        "919535075140",

        "919620952646"

    ]

    print("=" * 60)

    print(
        "📤 AI MIS WHATSAPP TEST SEND"
    )

    print("=" * 60)

    print(
        "Recipients:",
        recipients
    )

    results = []


    for recipient in recipients:

        try:

            success = (
                send_whatsapp_message(
                    recipient,
                    "🤖 AI MIS webhook "
                    "test message"
                )
            )

            results.append({

                "recipient":
                    recipient,

                "success":
                    success
            })

        except Exception as e:

            print(
                "❌ Test send error:",
                recipient,
                str(e)
            )

            results.append({

                "recipient":
                    recipient,

                "success":
                    False,

                "error":
                    str(e)
            })


    return {

        "success":
            True,

        "results":
            results

    }, 200


# =========================================================
# 🚀 LOCAL RUN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(

        host="0.0.0.0",

        port=port
    )
