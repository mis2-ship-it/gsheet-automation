
# =========================================================
# 📱 AI MIS WHATSAPP WEBHOOK
# =========================================================

from flask import Flask, request, jsonify
import os
import requests
import json
from datetime import datetime


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

GRAPH_API_VERSION = "v23.0"


# =========================================================
# 📊 LIVE SALES SNAPSHOT
# =========================================================

LIVE_SALES_DATA = {}


# =========================================================
# 🚀 STARTUP CHECK
# =========================================================

print("=" * 60)
print("🚀 AI MIS WHATSAPP WEBHOOK")
print("=" * 60)

print(
    "PHONE_NUMBER_ID exists :",
    bool(PHONE_NUMBER_ID)
)

print(
    "ACCESS_TOKEN exists    :",
    bool(ACCESS_TOKEN)
)

print(
    "VERIFY_TOKEN exists    :",
    bool(VERIFY_TOKEN)
)

print(
    "WHATSAPP_DATA_SECRET exists :",
    bool(WHATSAPP_DATA_SECRET)
)

print("=" * 60)

# =========================================================
# 🔐 DEBUG USER ACCESS
# =========================================================

def debug_user_access(sender):

    print("=" * 60)
    print("🔐 WHATSAPP ACCESS DEBUG")
    print("=" * 60)

    user = get_user_access(sender)

    if not user:

        print(
            "❌ User not mapped:",
            sender
        )

        print("=" * 60)

        return None

    print(
        "Sender :",
        sender
    )

    print(
        "Role   :",
        user.get("role")
    )

    print(
        "Region :",
        user.get("region")
    )

    print(
        "Patch  :",
        user.get("patch")
    )

    print(
        "Stores :",
        user.get("stores")
    )

    print("=" * 60)

    return user

# =========================================================
# 🏠 HOME
# =========================================================

@app.route("/", methods=["GET"])
def home():

    return (
        "AI MIS WhatsApp Webhook is running",
        200
    )


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
    print("🔐 META WEBHOOK VERIFICATION")
    print("Mode     :", mode)
    print(
        "Token OK :",
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

    return "Forbidden", 403


# =========================================================
# 📤 SEND WHATSAPP TEXT MESSAGE
# =========================================================

def send_whatsapp_message(
    recipient,
    message
):

    print("=" * 60)
    print("📤 SENDING WHATSAPP MESSAGE")
    print("To:", recipient)
    print("Message:")
    print(message)
    print("=" * 60)

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

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(
            "Meta Status   :",
            response.status_code
        )

        print(
            "Meta Response :",
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
# 📥 RECEIVE RISTA LIVE SALES SNAPSHOT
# =========================================================

@app.route(
    "/update-sales-data",
    methods=["POST"]
)
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

    print(
        "Data Secret configured:",
        bool(WHATSAPP_DATA_SECRET)
    )

    print(
        "Incoming Secret received:",
        bool(incoming_secret)
    )

    if not WHATSAPP_DATA_SECRET:

        print(
            "❌ WHATSAPP_DATA_SECRET "
            "is not configured"
        )

        return jsonify({

            "success": False,

            "error":
                "Server secret not configured"

        }), 500

    if incoming_secret != WHATSAPP_DATA_SECRET:

        print(
            "❌ Invalid WhatsApp data secret"
        )

        return jsonify({

            "success": False,

            "error":
                "Unauthorized"

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

            "error":
                "Empty JSON payload"

        }), 400

    # -----------------------------------------------------
    # DEBUG
    # -----------------------------------------------------

    print(
        "Received data keys:",
        list(data.keys())
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
        "Sections:",
        list(data.keys())
    )

    # -----------------------------------------------------
    # STORE SNAPSHOT
    # -----------------------------------------------------

    LIVE_SALES_DATA = data

    print(
        "✅ WhatsApp sales snapshot updated"
    )

    print(
        "LIVE_SALES_DATA keys:",
        list(LIVE_SALES_DATA.keys())
    )

    print("=" * 60)

    return jsonify({

        "success": True,

        "message":
            "Sales data updated successfully"

    }), 200


# =========================================================
# 🔍 CHECK CURRENT SALES SNAPSHOT
# =========================================================

@app.route(
    "/sales-data",
    methods=["GET"]
)
def sales_data():

    print("=" * 60)
    print("🔍 SALES DATA REQUEST")
    print("=" * 60)

    print(
        "LIVE_SALES_DATA empty:",
        not bool(LIVE_SALES_DATA)
    )

    print(
        "LIVE_SALES_DATA keys:",
        list(LIVE_SALES_DATA.keys())
    )

    if not LIVE_SALES_DATA:

        return jsonify({

            "success": False,

            "message":
                "No sales data available"

        }), 404

    return jsonify({

        "success": True,

        "data":
            LIVE_SALES_DATA

    }), 200


# =========================================================
# 📊 GET CURRENT FTD SALES
# =========================================================

def get_ftd_sales():

    print("=" * 60)
    print("📊 GET FTD SALES FROM SNAPSHOT")
    print("=" * 60)

    if not LIVE_SALES_DATA:

        print(
            "❌ LIVE_SALES_DATA is empty"
        )

        return None

    overall = LIVE_SALES_DATA.get(
        "overall",
        {}
    )

    result = {

        "date":
            LIVE_SALES_DATA.get(
                "date",
                datetime.now().strftime(
                    "%d-%b-%y"
                )
            ),

        "report_time":
            LIVE_SALES_DATA.get(
                "report_time",
                ""
            ),

        "net":
            float(
                overall.get(
                    "net",
                    0
                )
            ),

        "txn":
            float(
                overall.get(
                    "txn",
                    0
                )
            ),

        "aov":
            float(
                overall.get(
                    "aov",
                    0
                )
            ),

        "discount":
            float(
                overall.get(
                    "discount",
                    0
                )
            ),

        "gross":
            float(
                overall.get(
                    "gross",
                    0
                )
            ),

        "lw_net":
            float(
                overall.get(
                    "lw_net",
                    0
                )
            ),

        "lw_growth":
            float(
                overall.get(
                    "lw_growth",
                    0
                )
            )
    }

    print(
        "Date:",
        result["date"]
    )

    print(
        "Report Time:",
        result["report_time"]
    )

    print(
        "Net:",
        result["net"]
    )

    print(
        "Transactions:",
        result["txn"]
    )

    print(
        "AOV:",
        result["aov"]
    )

    print(
        "Discount:",
        result["discount"]
    )

    print("=" * 60)

    return result


# =========================================================
# 📊 GET BRAND DATA
# =========================================================

def get_brand_data():

    brands = LIVE_SALES_DATA.get(
        "brands",
        {}
    )

    print(
        "Brands available:",
        list(brands.keys())
    )

    return brands


# =========================================================
# 📊 GET SOURCE DATA
# =========================================================

def get_source_data():

    sources = LIVE_SALES_DATA.get(
        "sources",
        {}
    )

    print(
        "Sources available:",
        list(sources.keys())
    )

    return sources


# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES REQUEST")
    print("=" * 60)

    sales = get_ftd_sales()

    if not sales:

        send_whatsapp_message(

            sender,

            "⚠️ Sales data is not available right now."
        )

        return

    net = sales["net"]
    txn = sales["txn"]
    aov = sales["aov"]
    discount = sales["discount"]

    date = sales["date"]

    # -----------------------------------------------------
    # BRAND DATA
    # -----------------------------------------------------

    brands = get_brand_data()

    # -----------------------------------------------------
    # BUILD MESSAGE
    # -----------------------------------------------------

    reply = (

        f"📊 *AI MIS | FTD SALES*\n"

        f"{date}\n\n"

        f"💰 Net Revenue: "
        f"₹{net / 100000:.2f}L\n"

        f"🧾 Transactions: "
        f"{int(txn):,}\n"

        f"🧺 AOV: "
        f"₹{int(round(aov)):,}\n"

        f"📉 Discount: "
        f"{abs(discount):.1f}%"
    )

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

    if brands:

        reply += (
            "\n\n🏪 *Brand Contribution*"
        )

        brand_values = {}

        for brand_name, brand_data in brands.items():

            today_value = float(
                brand_data.get(
                    "today",
                    0
                )
            )

            brand_values[
                brand_name
            ] = today_value

        total_brand = sum(
            brand_values.values()
        )

        for brand_name, value in brand_values.items():

            contribution = (

                value
                /
                max(total_brand, 1)
                *
                100

            )

            reply += (

                f"\n• {brand_name}: "
                f"{contribution:.0f}%"
            )

    # -----------------------------------------------------
    # DEBUG
    # -----------------------------------------------------

    print(
        "WhatsApp FTD Reply:"
    )

    print(reply)

    print("=" * 60)

    # -----------------------------------------------------
    # SEND
    # -----------------------------------------------------

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 📊 SALES VS LAST WEEK
# =========================================================

def send_sales_vs_lw(sender):

    print("=" * 60)
    print("📊 SALES VS LAST WEEK")
    print("=" * 60)

    sales = get_ftd_sales()

    if not sales:

        send_whatsapp_message(

            sender,

            "⚠️ Sales data is not available right now."
        )

        return

    today_net = sales["net"]

    lw_net = sales["lw_net"]

    growth = (

        (
            today_net
            -
            lw_net
        )
        /
        max(lw_net, 1)
        *
        100
    )

    if growth > 5:

        performance = "🚀 Strong Growth"

    elif growth > 0:

        performance = "📈 Growth"

    elif growth < -5:

        performance = "🔻 Decline"

    else:

        performance = "➡️ Stable"

    reply = (

        f"📊 *AI MIS | SALES vs LW*\n"

        f"{sales['date']}\n\n"

        f"💰 Today: "
        f"₹{today_net / 100000:.2f}L\n"

        f"📊 Last Week: "
        f"₹{lw_net / 100000:.2f}L\n"

        f"📈 Growth: "
        f"{growth:+.1f}%\n\n"

        f"🧠 Performance: "
        f"{performance}"
    )

    print(reply)

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 👋 PROCESS MESSAGE
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
    print("🧠 PROCESSING MESSAGE")
    print("Sender     :", sender)
    print("Original   :", message_text)
    print("Normalized :", message)
    print("=" * 60)

    # =====================================================
    # GREETING
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

            "You can try:\n"

            "📊 *sales*\n"

            "📊 *sales vs lw*\n"

            "❓ *help*"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # =====================================================
    # HELP
    # =====================================================

    if message == "help":

        reply = (

            "🤖 *AI MIS WhatsApp*\n\n"

            "Available commands:\n\n"

            "📊 *sales*\n"

            "📊 *sales today*\n"

            "📈 *sales vs lw*\n"

            "❓ *help*"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # =====================================================
    # SALES
    # =====================================================

    sales_keywords = [

        "sales",
        "sales today",
        "today sales",
        "today's sales",
        "todays sales",
        "ftd",
        "ftd sales",
        "sales for today",
        "today's sale",
        "todays sale",

        "what is today's sales",
        "what is todays sales",
        "what are today's sales",
        "what are todays sales",

        "how was the sales today",
        "how was sales today",
        "how are sales today",
        "how was the sale today",
        "how are the sales today",
        "how is the sales today"

    ]

    if message in sales_keywords:

        print(
            "📊 FTD SALES COMMAND DETECTED"
        )

        send_ftd_sales(
            sender
        )

        return

    # =====================================================
    # SALES VS LW
    # =====================================================

    sales_lw_keywords = [

        "sales vs lw",
        "sales vs last week",
        "sales versus last week",
        "today vs last week",
        "today vs lw",
        "last week sales",
        "compare sales",
        "sales comparison"

    ]

    if message in sales_lw_keywords:

        print(
            "📊 SALES VS LW COMMAND DETECTED"
        )

        send_sales_vs_lw(
            sender
        )

        return

    # =====================================================
    # NATURAL SALES QUESTION
    # =====================================================

    if (

        "sales" in message
        and
        "today" in message

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
    # UNKNOWN MESSAGE
    # =====================================================

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
    print("📩 WHATSAPP WEBHOOK RECEIVED")
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

    if data.get(
        "object"
    ) != "whatsapp_business_account":

        print(
            "⚠️ Not a WhatsApp Business "
            "Account event"
        )

        return (
            "EVENT_RECEIVED",
            200
        )

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

            # =================================================
            # MESSAGE EVENT
            # =================================================

            if field == "messages":

                messages = value.get(
                    "messages",
                    []
                )

                print(
                    "Number of messages:",
                    len(messages)
                )

                for message_data in messages:

                    message_type = (
                        message_data.get(
                            "type"
                        )
                    )

                    sender = (
                        message_data.get(
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

                    # -----------------------------------------
                    # TEXT
                    # -----------------------------------------

                    if message_type == "text":

                        text_data = (
                            message_data.get(
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
                            and
                            message_text
                        ):

                            process_message(
                                sender,
                                message_text
                            )

                    # -----------------------------------------
                    # NON-TEXT
                    # -----------------------------------------

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

            # =================================================
            # STATUS / OTHER EVENTS
            # =================================================

            else:

                print(
                    "ℹ️ Other webhook event:",
                    field
                )

    return (
        "EVENT_RECEIVED",
        200
    )


# =========================================================
# 🧪 TEST SEND
# =========================================================

@app.route(
    "/test-send",
    methods=["GET"]
)
def test_send():

    recipients = [

        "919750820509",
        "919535075140",
        "918892390985",
        "919620952646"

    ]

    print("=" * 60)
    print("📤 AI MIS WHATSAPP TEST SEND")
    print("=" * 60)

    success_count = 0
    failed_count = 0

    for recipient in recipients:

        success = send_whatsapp_message(

            recipient,

            "🤖 AI MIS webhook test message"
        )

        if success:

            success_count += 1

        else:

            failed_count += 1

    print("=" * 60)

    print(
        "WhatsApp Success:",
        success_count
    )

    print(
        "WhatsApp Failed:",
        failed_count
    )

    print("=" * 60)

    return jsonify({

        "success":
            failed_count == 0,

        "results": {

            "success_count":
                success_count,

            "failed_count":
                failed_count,

            "recipients":
                recipients
        }

    }), 200


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
