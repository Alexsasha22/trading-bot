import os
from flask import Flask, request, jsonify

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

@app.get("/")
def home():
    return "EURUSD bot online", 200

@app.get("/health")
def health():
    return "OK", 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}

    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    required = ["action", "volume", "sl", "tp1", "tp2"]
    missing = [x for x in required if x not in data]

    if missing:
        return jsonify({"error": "Missing fields", "fields": missing}), 400

    action = str(data["action"]).upper()

    if action not in ("BUY", "SELL"):
        return jsonify({"error": "Invalid action"}), 400

    return jsonify({
        "status": "received",
        "action": action,
        "volume": data["volume"],
        "sl": data["sl"],
        "tp1": data["tp1"],
        "tp2": data["tp2"]
    }), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
