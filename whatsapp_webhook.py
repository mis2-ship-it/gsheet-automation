import os
from flask import Flask, request

app = Flask(__name__)

VERIFY_TOKEN = os.environ.get(
    "WHATSAPP_VERIFY_TOKEN",
    "AI_MIS_WHATSAPP_VERIFY_2026"
)


# =========================================================
# META WEBHOOK VERIFICATION
# =========================================================

@app.route("/webhook", methods=["GET"])
def verify_webhook():

    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    print("=" * 60)
    print("WhatsApp Webhook Verification")
    print("=" * 60)

    if mode == "subscribe" and token == VERIFY_TOKEN:

        print("✅ Webhook verification successful")

        return challenge, 200

    print("❌ Webhook verification failed")

    return "Verification failed", 403


# =========================================================
# WHATSAPP STATUS WEBHOOK
# =========================================================

@app.route("/webhook", methods=["POST"])
def receive_webhook():

    data = request.get_json(silent=True)

    print("=" * 60)
    print("WHATSAPP WEBHOOK RECEIVED")
    print("=" * 60)

    print(data)

    print("=" * 60)

    return "EVENT_RECEIVED", 200


# =========================================================
# HEALTH CHECK
# =========================================================

@app.route("/", methods=["GET"])
def home():

    return "AI MIS WhatsApp Webhook is running", 200


# =========================================================
# START SERVER
# =========================================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    print("=" * 60)
    print("AI MIS WhatsApp Webhook")
    print("Port:", port)
    print("=" * 60)

    app.run(
        host="0.0.0.0",
        port=port
    )
