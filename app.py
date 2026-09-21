import os
import logging
from flask import Flask, request, jsonify

# Initialize Flask App
app = Flask(__name__)
logger = logging.getLogger(__name__)

# Fetch secret key from environment variable
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "DSR_Secret_Pass_998877")

# Root route (to verify server health)
@app.route("/refresh-dsr", methods=["GET", "POST"])
def refresh_dsr():
    if request.method == "GET":
        return jsonify({"status": "active", "message": "DSR Refresh Endpoint is live!"}), 200

    # Authorization Check
    webhook_secret = os.getenv("WEBHOOK_SECRET", "DSR_Secret_Pass_998877")
    auth_header = request.headers.get("Authorization")
    
    if auth_header != f"Bearer {webhook_secret}":
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    try:
        from dsr_dashboard import DSRDashboard  # Ensure this matches your dashboard class file name
        
        dashboard = DSRDashboard()
        success = dashboard.update_google_sheet()

        if success:
            return jsonify({"status": "success", "message": "Dashboard updated successfully!"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to update Google Sheet."}), 500

    except Exception as e:
        app.logger.exception("Error updating DSR Dashboard")
        return jsonify({"status": "error", "message": str(e)}), 500
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
