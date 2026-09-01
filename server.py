import os
import requests
from flask import Flask, request, jsonify, redirect

app = Flask(__name__)

CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CTRADER_REDIRECT_URI")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

access_token = None
refresh_token = None


@app.get("/")
def home():
    return "EURUSD bot online", 200


@app.get("/health")
def health():
    return jsonify({
        "status": "OK",
        "ctrader_configured": bool(
            CLIENT_ID and CLIENT_SECRET and REDIRECT_URI
        ),
        "authenticated": access_token is not None
    }), 200


@app.get("/auth")
def auth():
    if not CLIENT_ID or not REDIRECT_URI:
        return jsonify({"error": "cTrader OAuth variables missing"}), 500

    url = (
        "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=trading"
        "&product=web"
    )

    return redirect(url)


@app.get("/callback")
def callback():
    global access_token, refresh_token

    code = request.args.get("code")

    if not code:
        return jsonify({
            "error": "No authorization code received",
            "details": request.args.to_dict()
        }), 400

    response = requests.get(
        "https://openapi.ctrader.com/apps/token",
        params={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        timeout=20
    )

    data = response.json()

    if response.status_code != 200 or not data.get("accessToken"):
        return jsonify({
            "error": "cTrader token exchange failed",
            "response": data
        }), 400

    access_token = data["accessToken"]
    refresh_token = data.get("refreshToken")

    return jsonify({
        "status": "cTrader authentication successful",
        "token_received": True,
        "message": "The cTrader account authorization is now connected."
    }), 200


@app.post("/webhook")
def webhook():

    if WEBHOOK_SECRET:
        secret = request.headers.get("X-Webhook-Secret")

        if secret != WEBHOOK_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True)

    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    action = str(data.get("action", "")).upper()

    if action not in ("BUY", "SELL"):
        return jsonify({"error": "Invalid action"}), 400

    return jsonify({
        "status": "received",
        "action": action,
        "symbol": data.get("symbol"),
        "volume": data.get("volume"),
        "sl": data.get("sl"),
        "tp1": data.get("tp1"),
        "tp2": data.get("tp2"),
        "ctrader_authenticated": access_token is not None
    }), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
