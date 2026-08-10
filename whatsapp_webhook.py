from flask import Flask, request
import os
import requests

# =====================================================
# FLASK APP
# =====================================================

app = Flask(__name__)

# =====================================================
# ENVIRONMENT VARIABLES
# =====================================================

VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN")
PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID")
ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")

# =====================================================
# HOME
# =====================================================

@app.route("/", methods=["GET"])
def home():
    return "AI MIS WhatsApp Webhook is running", 200


# =====================================================
# META WEBHOOK VERIFICATION
# =====================================================

@app.route("/webhook", methods=["GET"])
def verify_webhook():

    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("✅ META WEBHOOK VERIFIED")
        return challenge, 200

    print("❌ WEBHOOK VERIFICATION FAILED")
    return "Forbidden", 403


# =====================================================
# META WEBHOOK RECEIVER
# =====================================================

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.get_json(silent=True)

    print("=" * 60)
    print("📩 WHATSAPP WEBHOOK RECEIVED")
    print(data)
    print("=" * 60)

    return "EVENT_RECEIVED", 200


# =====================================================
# TEST SEND
# =====================================================

@app.route("/test-send", methods=["GET"])
def test_send():

    RECIPIENT = "919750820509"

    url = (
        f"https://graph.facebook.com/v23.0/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": RECIPIENT,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": "AI MIS webhook test message"
        }
    }

    print("=" * 60)
    print("📤 SENDING WHATSAPP TEST")
    print("To:", RECIPIENT)
    print("=" * 60)

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=30
    )

    print("Status:", response.status_code)
    print("Response:", response.text)
    print("=" * 60)

    return response.text, response.status_code


# =====================================================
# LOCAL RUN
# =====================================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port
    )
