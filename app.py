# app.py
from flask import Flask, jsonify, request
import os
import json
import logging
# Import your existing DSRDashboard class here
# from your_module import DSRDashboard 

app = Flask(__name__)
logger = logging.getLogger(__name__)

# Secret key to prevent unauthorized access
WEBHOOK_SECRET = "DSR_Secret_Pass_998877"

@app.route("/refresh-dsr", methods=["POST"])
def refresh_dsr():
    # 1. Verify Authorization Token
    auth_header = request.headers.get("Authorization")
    if auth_header != f"Bearer {WEBHOOK_SECRET}":
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    try:
        logger.info("⚡ Webhook triggered from Google Sheets. Running DSR update...")
        
        # 2. Instantiate and run Google Sheet update
        dashboard = DSRDashboard()
        success = dashboard.update_google_sheet()

        if success:
            return jsonify({"status": "success", "message": "Dashboard updated successfully!"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to update dashboard. Check Python logs."}), 500

    except Exception as e:
        logger.exception("Error during webhook execution")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Runs on port 5000
    app.run(host="0.0.0.0", port=5000)
