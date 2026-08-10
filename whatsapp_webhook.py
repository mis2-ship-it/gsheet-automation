
from flask import Flask, request
import os
import requests
import json
from datetime import datetime


# =========================================================
# 📱 AI MIS WHATSAPP WEBHOOK
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

GRAPH_API_VERSION = "v23.0"


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

print("=" * 60)


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
# 📤 SEND WHATSAPP TEXT MESSAGE
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
# 📊 GET FTD SALES
# =========================================================
#
# ⚠️ CURRENTLY TEST DATA
#
# We will replace ONLY this function with
# the actual Rista / Google Sheet data logic.
#
# =========================================================

def get_ftd_sales():

    print("=" * 60)
    print("📊 GET FTD SALES")
    print("=" * 60)

    # -----------------------------------------------------
    # TEST VALUES
    # -----------------------------------------------------

    today_net = 469000
    today_txn = 1809
    today_aov = 259
    today_discount = 29.9

    frozen_bottle = 386000
    madno = 60000
    boba_bar = 16000

    print(
        "Net Revenue :",
        today_net
    )

    print(
        "Transactions:",
        today_txn
    )

    print(
        "AOV         :",
        today_aov
    )

    print(
        "Discount    :",
        today_discount
    )

    print(
        "Frozen Bottle:",
        frozen_bottle
    )

    print(
        "Madno:",
        madno
    )

    print(
        "Boba Bar:",
        boba_bar
    )

    print("=" * 60)

    return {

        "net":
            today_net,

        "txn":
            today_txn,

        "aov":
            today_aov,

        "discount":
            today_discount,

        "frozen_bottle":
            frozen_bottle,

        "madno":
            madno,

        "boba_bar":
            boba_bar
    }


# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES REQUEST")
    print("=" * 60)

    # -----------------------------------------------------
    # GET SALES
    # -----------------------------------------------------

    sales = get_ftd_sales()

    net = sales["net"]

    txn = sales["txn"]

    aov = sales["aov"]

    discount = sales["discount"]

    frozen_bottle = (
        sales["frozen_bottle"]
    )

    madno = (
        sales["madno"]
    )

    boba_bar = (
        sales["boba_bar"]
    )

    # -----------------------------------------------------
    # BRAND TOTAL
    # -----------------------------------------------------

    total_brand = (

        frozen_bottle
        + madno
        + boba_bar

    )

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

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
        datetime.now().strftime(
            "%d-%b-%y"
        )
    )

    # -----------------------------------------------------
    # WHATSAPP MESSAGE
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
# 🧠 PROCESS INCOMING MESSAGE
# =========================================================

def process_message(
    sender,
    message_text
):

    # =====================================================
    # NORMALIZE MESSAGE
    # =====================================================

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

            "👋 Hi! Welcome to "
            "AI MIS WhatsApp.\n\n"

            "You can try:\n"
            "📊 sales today\n"
            "📊 sales\n"
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

            "📊 *sales today*\n"
            "📊 *sales*\n"
            "❓ *help*"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return


    # =====================================================
    # 📊 FTD SALES KEYWORDS
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


    # =====================================================
    # 📊 EXACT SALES COMMAND
    # =====================================================

    if message in sales_keywords:

        print(
            "📊 FTD SALES COMMAND DETECTED"
        )

        send_ftd_sales(
            sender
        )

        return


    # =====================================================
    # 📊 NATURAL SALES QUESTIONS
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
    # ❓ UNKNOWN MESSAGE
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

        !=

        "whatsapp_business_account"

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
            # 📩 MESSAGE EVENT
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


                    # =========================================
                    # 💬 TEXT MESSAGE
                    # =========================================

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


                    # =========================================
                    # 📷 NON-TEXT MESSAGE
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


            # =================================================
            # OTHER EVENTS
            # =================================================

            else:

                print(
                    "ℹ️ Other webhook event:",
                    field
                )


    # =====================================================
    # ALWAYS RETURN 200
    # =====================================================

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

    RECIPIENTS = [
        "919750820509",
        "919535075140",
        "918892390985",
        "919620952646",
        "918553666666"
    ]

    print("=" * 60)
    print(
        "📤 AI MIS WHATSAPP TEST SEND"
    )
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
        "Sending to:",
        recipients
    )

    print(
        "Phone Number ID:",
        PHONE_NUMBER_ID
    )


    # =====================================================
    # VALIDATION
    # =====================================================

    if not PHONE_NUMBER_ID:

        return {

            "success":
                False,

            "error":
                "WHATSAPP_PHONE_NUMBER_ID "
                "is missing in Render"

        }, 500


    if not ACCESS_TOKEN:

        return {

            "success":
                False,

            "error":
                "WHATSAPP_ACCESS_TOKEN "
                "is missing in Render"

        }, 500


    # =====================================================
    # WHATSAPP API
    # =====================================================

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


    success_count = 0

    failed_count = 0


    # =====================================================
    # SEND TO EACH NUMBER
    # =====================================================

    for recipient in recipients:

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
                    "🤖 AI MIS webhook "
                    "test message"

            }

        }


        print(
            "Sending to:",
            recipient
        )


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

                success_count += 1

                print(
                    "✅ Sent:",
                    recipient
                )

            else:

                failed_count += 1

                print(
                    "❌ Failed:",
                    recipient
                )


        except Exception as e:

            failed_count += 1

            print(
                "❌ Error:",
                recipient,
                str(e)
            )


    # =====================================================
    # FINAL RESULT
    # =====================================================

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


    return {

        "success":
            failed_count == 0,

        "success_count":
            success_count,

        "failed_count":
            failed_count,

        "recipients":
            recipients

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
