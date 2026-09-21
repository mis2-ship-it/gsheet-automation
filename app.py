import os
import logging
from flask import Flask, request, jsonify

# Initialize Flask App
app = Flask(__name__)
logger = logging.getLogger(__name__)

# Fetch secret key from environment variable
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "DSR_Secret_Pass_998877")

# Root route (to verify server health)
@app.route("/", methods=["GET", "HEAD"])
def index():
    return "AI MIS WhatsApp Webhook Service is Running!", 200

# DSR Refresh Webhook Endpoint
@app.route("/refresh-dsr", methods=["POST"])
def refresh_dsr():
    auth_header = request.headers.get("Authorization")
    if auth_header != f"Bearer {WEBHOOK_SECRET}":
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    try:
        # Import your DSRDashboard class dynamically
        from dsr_dashboard import DSRDashboard  # Ensure this matches your dashboard file name
        
        dashboard = DSRDashboard()
        success = dashboard.update_google_sheet()

        if success:
            return jsonify({"status": "success", "message": "Dashboard updated successfully!"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to update Google Sheet."}), 500

    except Exception as e:
        logger.exception("Error during DSR refresh execution")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
