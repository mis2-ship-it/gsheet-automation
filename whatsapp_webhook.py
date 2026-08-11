from flask import Flask, request
import os
import requests
import json

# =========================================================
# FLASK APP
# =========================================================

app = Flask(__name__)

# =========================================================
# ENVIRONMENT VARIABLES
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

GRAPH_API_VERSION = "v23.0"

# =========================================================
# BASIC CHECK
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

print("=" * 60)


# =========================================================
# HOME
# =========================================================

@app.route("/", methods=["GET"])
def home():

    return (
        "AI MIS WhatsApp Webhook is running",
        200
    )


# =========================================================
# META WEBHOOK VERIFICATION
# =========================================================

@app.route("/webhook", methods=["GET"])
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
# SEND WHATSAPP TEXT MESSAGE
# =========================================================

def send_whatsapp_message(
    recipient,
    message
):

    if not PHONE_NUMBER_ID:

        print(
            "❌ WHATSAPP_PHONE_NUMBER_ID "
            "is missing"
        )

        return False

    if not ACCESS_TOKEN:

        print(
            "❌ WHATSAPP_ACCESS_TOKEN "
            "is missing"
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
    print("📤 SENDING WHATSAPP MESSAGE")
    print("To:", recipient)
    print("Message:")
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
            "Meta Status   :",
            response.status_code
        )

        print(
            "Meta Response :",
            response.text
        )

        print("=" * 60)

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
# 📊 FTD SALES
# =========================================================

def send_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES REQUEST")
    print("=" * 60)

    # -----------------------------------------------------
    # GET FTD DATA
    # -----------------------------------------------------

    sales = get_ftd_sales()

    net = sales["net"]
    txn = sales["txn"]
    aov = sales["aov"]
    discount = sales["discount"]

    frozen_bottle = sales[
        "frozen_bottle"
    ]

    madno = sales[
        "madno"
    ]

    boba_bar = sales[
        "boba_bar"
    ]

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

    total_brand = (
        frozen_bottle
        + madno
        + boba_bar
    )

    frozen_pct = (
        frozen_bottle
        / max(total_brand, 1)
    ) * 100

    madno_pct = (
        madno
        / max(total_brand, 1)
    ) * 100

    boba_pct = (
        boba_bar
        / max(total_brand, 1)
    ) * 100

    # -----------------------------------------------------
    # DATE
    # -----------------------------------------------------

    today_date = (
        business_day.strftime(
            "%d-%b-%y"
        )
    )

    # -----------------------------------------------------
    # WHATSAPP RESPONSE
    # -----------------------------------------------------

    reply = (

        f"📊 *AI MIS | FTD SALES*\n"
        f"{today_date}\n\n"

        f"💰 Net Revenue: "
        f"₹{net / 100000:.2f}L\n"

        f"🧾 Transactions: "
        f"{int(txn):,}\n"

        f"🧺 AOV: "
        f"₹{int(round(aov)):,}\n"

        f"📉 Discount: "
        f"{abs(discount):.1f}%\n\n"

        f"🏪 *Brand Contribution*\n"

        f"🍶 Frozen Bottle: "
        f"{frozen_pct:.0f}%\n"

        f"🥤 Madno: "
        f"{madno_pct:.0f}%\n"

        f"🧋 Boba Bar: "
        f"{boba_pct:.0f}%"
    )

    print(
        "WhatsApp FTD Reply:"
    )

    print(reply)

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 📅 YESTERDAY SALES
# =========================================================

def send_yesterday_sales(sender):

    print("=" * 60)
    print("📅 YESTERDAY SALES REQUEST")
    print("=" * 60)

    sales = get_yesterday_sales()

    net = sales["net"]
    txn = sales["txn"]
    aov = sales["aov"]
    discount = sales["discount"]

    frozen_bottle = sales[
        "frozen_bottle"
    ]

    madno = sales[
        "madno"
    ]

    boba_bar = sales[
        "boba_bar"
    ]

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

    total_brand = (
        frozen_bottle
        + madno
        + boba_bar
    )

    frozen_pct = (
        frozen_bottle
        / max(total_brand, 1)
    ) * 100

    madno_pct = (
        madno
        / max(total_brand, 1)
    ) * 100

    boba_pct = (
        boba_bar
        / max(total_brand, 1)
    ) * 100

    # -----------------------------------------------------
    # YESTERDAY DATE
    # -----------------------------------------------------

    yesterday_date = (
        yesterday_business_day.strftime(
            "%d-%b-%y"
        )
    )

    # -----------------------------------------------------
    # RESPONSE
    # -----------------------------------------------------

    reply = (

        f"📊 *AI MIS | YESTERDAY SALES*\n"
        f"{yesterday_date}\n\n"

        f"💰 Net Revenue: "
        f"₹{net / 100000:.2f}L\n"

        f"🧾 Transactions: "
        f"{int(txn):,}\n"

        f"🧺 AOV: "
        f"₹{int(round(aov)):,}\n"

        f"📉 Discount: "
        f"{abs(discount):.1f}%\n\n"

        f"🏪 *Brand Contribution*\n"

        f"🍶 Frozen Bottle: "
        f"{frozen_pct:.0f}%\n"

        f"🥤 Madno: "
        f"{madno_pct:.0f}%\n"

        f"🧋 Boba Bar: "
        f"{boba_pct:.0f}%"
    )

    print(
        "WhatsApp Yesterday Reply:"
    )

    print(reply)

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 📈 SALES VS LAST WEEK
# =========================================================

def send_sales_vs_lw(sender):

    print("=" * 60)
    print("📈 SALES VS LAST WEEK REQUEST")
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

    # -----------------------------------------------------
    # GROWTH ICON
    # -----------------------------------------------------

    if growth > 0:

        growth_icon = "📈"

    elif growth < 0:

        growth_icon = "🔻"

    else:

        growth_icon = "➡️"

    # -----------------------------------------------------
    # RESPONSE
    # -----------------------------------------------------

    today_date = (
        business_day.strftime(
            "%d-%b-%y"
        )
    )

    reply = (

        f"📊 *AI MIS | SALES VS LW*\n"
        f"{today_date}\n\n"

        f"💰 *NET REVENUE*\n"

        f"🟢 Today: "
        f"₹{today_net / 100000:.2f}L\n"

        f"🔵 Last Week: "
        f"₹{lw_net / 100000:.2f}L\n"

        f"{growth_icon} Growth: "
        f"{growth:+.1f}%\n\n"

        f"🧾 *TRANSACTIONS*\n"

        f"🟢 Today: "
        f"{int(today_txn):,}\n"

        f"🔵 Last Week: "
        f"{int(lw_txn):,}\n\n"

        f"🧺 *AOV*\n"

        f"🟢 Today: "
        f"₹{int(round(today_aov)):,}\n"

        f"🔵 Last Week: "
        f"₹{int(round(lw_aov)):,}\n\n"

        f"📉 *DISCOUNT*\n"

        f"🟢 Today: "
        f"{abs(today_discount):.1f}%\n"

        f"🔵 Last Week: "
        f"{abs(lw_discount):.1f}%"
    )

    print(
        "WhatsApp Sales vs LW Reply:"
    )

    print(reply)

    send_whatsapp_message(
        sender,
        reply
    )

def send_brand_performance(sender):

    print("=" * 60)
    print("🏪 BRAND PERFORMANCE REQUEST")
    print("=" * 60)

    sales = get_ftd_sales()

    frozen_bottle = sales["frozen_bottle"]
    madno = sales["madno"]
    boba_bar = sales["boba_bar"]

    total_brand = (
        frozen_bottle
        + madno
        + boba_bar
    )

    frozen_pct = (
        frozen_bottle / max(total_brand, 1)
    ) * 100

    madno_pct = (
        madno / max(total_brand, 1)
    ) * 100

    boba_pct = (
        boba_bar / max(total_brand, 1)
    ) * 100

    today_date = business_day.strftime(
        "%d-%b-%y"
    )

    reply = (
        f"🏪 *AI MIS | BRAND PERFORMANCE*\n"
        f"{today_date}\n\n"

        f"🍶 *Frozen Bottle*\n"
        f"💰 Revenue: ₹{frozen_bottle / 100000:.2f}L\n"
        f"📊 Contribution: {frozen_pct:.0f}%\n\n"

        f"🥤 *Madno*\n"
        f"💰 Revenue: ₹{madno / 100000:.2f}L\n"
        f"📊 Contribution: {madno_pct:.0f}%\n\n"

        f"🧋 *Boba Bar*\n"
        f"💰 Revenue: ₹{boba_bar / 100000:.2f}L\n"
        f"📊 Contribution: {boba_pct:.0f}%"
    )

    print("WhatsApp Brand Reply:")
    print(reply)

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 🧠 PROCESS INCOMING MESSAGE
# =========================================================

def process_message(
    sender,
    message_text
):

    # -----------------------------------------------------
    # NORMALIZE MESSAGE
    # -----------------------------------------------------

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
            "👋 Hi! Welcome to AI MIS WhatsApp.\n\n"

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
        "how was yesterday's sales",
        "how were yesterday sales",
        "what were yesterday sales"

    ]

    # -----------------------------------------------------
    # EXACT MATCH
    # -----------------------------------------------------

    if message in yesterday_keywords:

        print(
            "📅 YESTERDAY SALES "
            "COMMAND DETECTED"
        )

        try:

            send_yesterday_sales(
                sender
            )

            print(
                "✅ send_yesterday_sales() completed"
            )

        except Exception as e:

            print(
                "❌ send_yesterday_sales() ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Yesterday Sales report."
            )

        return

    # -----------------------------------------------------
    # NATURAL QUESTION
    # -----------------------------------------------------

    if (
        "sales" in message
        and "yesterday" in message
    ):

        print(
            "📅 NATURAL YESTERDAY "
            "SALES QUESTION DETECTED"
        )

        try:

            send_yesterday_sales(
                sender
            )

            print(
                "✅ Natural Yesterday Sales completed"
            )

        except Exception as e:

            print(
                "❌ Natural Yesterday Sales ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Yesterday Sales report."
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
        "compare sales last week",
        "compare sales with last week",
        "how are sales vs last week",
        "how is sales vs last week",
        "how was sales vs last week",
        "sales vs lw"

    ]

    # -----------------------------------------------------
    # EXACT MATCH
    # -----------------------------------------------------

    if message in sales_vs_lw_keywords:

        print(
            "📈 SALES VS LW "
            "COMMAND DETECTED"
        )

        print(
            "➡️ Calling send_sales_vs_lw()"
        )

        try:

            send_sales_vs_lw(
                sender
            )

            print(
                "✅ send_sales_vs_lw() completed"
            )

        except Exception as e:

            print(
                "❌ send_sales_vs_lw() ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Sales vs Last Week report."
            )

        return

    # -----------------------------------------------------
    # NATURAL SALES VS LW QUESTIONS
    # -----------------------------------------------------

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

        try:

            send_sales_vs_lw(
                sender
            )

            print(
                "✅ Natural Sales vs LW completed"
            )

        except Exception as e:

            print(
                "❌ Natural Sales vs LW ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Sales vs Last Week report."
            )

        return

    # =====================================================
    # 🏪 BRAND PERFORMANCE
    # =====================================================

    brand_keywords = [

        "brand sales",
        "brand performance",
        "sales by brand",
        "brand wise sales",
        "brand wise performance",
        "brand"

    ]

    # -----------------------------------------------------
    # EXACT BRAND COMMAND
    # -----------------------------------------------------

    if message in brand_keywords:

        print(
            "🏪 BRAND PERFORMANCE "
            "COMMAND DETECTED"
        )

        print(
            "➡️ Calling send_brand_performance()"
        )

        try:

            send_brand_performance(
                sender
            )

            print(
                "✅ send_brand_performance() completed"
            )

        except Exception as e:

            print(
                "❌ send_brand_performance() ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Brand Performance report."
            )

        return

    # -----------------------------------------------------
    # NATURAL BRAND QUESTIONS
    # -----------------------------------------------------

    if (
        "brand" in message
        and "sales" in message
    ):

        print(
            "🏪 NATURAL BRAND PERFORMANCE "
            "QUESTION DETECTED"
        )

        try:

            send_brand_performance(
                sender
            )

            print(
                "✅ Natural Brand Performance completed"
            )

        except Exception as e:

            print(
                "❌ Natural Brand Performance ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating Brand Performance report."
            )

        return

    # =====================================================
    # 📊 FTD SALES
    # =====================================================

    sales_keywords = [

        "sales",
        "sales today",
        "today sales",
        "how was the sales today",
        "how was sales today",
        "how are sales today",
        "how was the sale today",
        "how are the sales today",
        "how is the sales today",
        "what is today's sales",
        "what is todays sales",
        "what are today's sales",
        "what are todays sales",
        "today's sales",
        "todays sales",
        "ftd",
        "ftd sales",
        "sales for today",
        "today's sale",
        "todays sale"

    ]

    # -----------------------------------------------------
    # EXACT MATCH
    # -----------------------------------------------------

    if message in sales_keywords:

        print(
            "📊 FTD SALES "
            "COMMAND DETECTED"
        )

        print(
            "➡️ Calling send_ftd_sales()"
        )

        try:

            send_ftd_sales(
                sender
            )

            print(
                "✅ send_ftd_sales() completed"
            )

        except Exception as e:

            print(
                "❌ send_ftd_sales() ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating FTD Sales report."
            )

        return

    # -----------------------------------------------------
    # NATURAL SALES QUESTIONS
    # -----------------------------------------------------

    if (
        "sales" in message
        and "today" in message
    ):

        print(
            "📊 NATURAL FTD SALES "
            "QUESTION DETECTED"
        )

        try:

            send_ftd_sales(
                sender
            )

            print(
                "✅ Natural FTD Sales completed"
            )

        except Exception as e:

            print(
                "❌ Natural FTD Sales ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                "❌ Error while generating FTD Sales report."
            )

        return

    # =====================================================
    # ❌ UNKNOWN MESSAGE
    # =====================================================

    print(
        "❓ UNKNOWN COMMAND:",
        message
    )

    reply = (

        "🤖 AI MIS received your message:\n\n"

        f"\"{message_text}\"\n\n"

        "Type *help* to see available commands."
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

    # =====================================================
    # SAFETY CHECK
    # =====================================================

    if not data:

        print(
            "⚠️ Empty webhook payload"
        )

        return (
            "EVENT_RECEIVED",
            200
        )

    # =====================================================
    # META OBJECT CHECK
    # =====================================================

    if (
        data.get("object")
        != "whatsapp_business_account"
    ):

        print(
            "⚠️ Not a WhatsApp "
            "Business Account event"
        )

        return (
            "EVENT_RECEIVED",
            200
        )

    # =====================================================
    # ENTRY
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

            # =================================================
            # MESSAGE EVENT
            # =================================================

            if field != "messages":

                print(
                    "ℹ️ Other webhook event:",
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

            # =================================================
            # PROCESS MESSAGES
            # =================================================

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

                # =============================================
                # TEXT MESSAGE
                # =============================================

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
                                "✅ process_message() completed"
                            )

                        except Exception as e:

                            print(
                                "❌ process_message() ERROR:",
                                str(e)
                            )

                            send_whatsapp_message(
                                sender,
                                "❌ AI MIS encountered an error while processing your request."
                            )

                # =============================================
                # NON-TEXT MESSAGE
                # =============================================

                else:

                    print(
                        "⚠️ Non-text message received:",
                        message_type
                    )

                    if sender:

                        send_whatsapp_message(
                            sender,

                            "🤖 AI MIS currently "
                            "supports text messages only."
                        )

    # =====================================================
    # ALWAYS RETURN 200 TO META
    # =====================================================

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
        "Sending to:",
        recipients
    )

    print(
        "PHONE_NUMBER_ID exists:",
        bool(PHONE_NUMBER_ID)
    )

    print(
        "ACCESS_TOKEN exists:",
        bool(ACCESS_TOKEN)
    )

    print(
        "VERIFY_TOKEN exists:",
        bool(VERIFY_TOKEN)
    )

    # =====================================================
    # CONFIG CHECK
    # =====================================================

    if not PHONE_NUMBER_ID:

        return {
            "success": False,
            "error":
                "WHATSAPP_PHONE_NUMBER_ID "
                "is missing in Render"
        }, 500

    if not ACCESS_TOKEN:

        return {
            "success": False,
            "error":
                "WHATSAPP_ACCESS_TOKEN "
                "is missing in Render"
        }, 500

    # =====================================================
    # SEND TO ALL NUMBERS
    # =====================================================

    results = []

    for recipient in recipients:

        print(
            "➡️ Sending test message to:",
            recipient
        )

        try:

            success = send_whatsapp_message(
                recipient,
                "🤖 AI MIS webhook test message"
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

    # =====================================================
    # RESULT
    # =====================================================

    return {

        "success": True,

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
