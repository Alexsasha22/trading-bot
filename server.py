import os
import threading
import requests

from flask import Flask, request, jsonify, redirect

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
)

from twisted.internet import reactor


app = Flask(__name__)

CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CTRADER_REDIRECT_URI")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

access_token = None
refresh_token = None

ctrader_client = None

ctrader_connected = False
application_authenticated = False
account_authenticated = False

account_id = None


def start_ctrader():
    global ctrader_client

    if ctrader_client is not None:
        return

    print("Starting cTrader DEMO connection...")

    ctrader_client = Client(
        EndPoints.PROTOBUF_DEMO_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol
    )

    ctrader_client.setConnectedCallback(on_ctrader_connected)
    ctrader_client.setDisconnectedCallback(on_ctrader_disconnected)
    ctrader_client.setMessageReceivedCallback(on_ctrader_message)

    ctrader_client.startService()

    threading.Thread(
        target=reactor.run,
        kwargs={"installSignalHandlers": False},
        daemon=True
    ).start()


def on_ctrader_connected(client):
    global ctrader_connected

    ctrader_connected = True

    print("cTrader DEMO connected")

    request = ProtoOAApplicationAuthReq()

    request.clientId = CLIENT_ID
    request.clientSecret = CLIENT_SECRET

    client.send(request).addErrback(on_ctrader_error)


def on_ctrader_disconnected(client, reason):
    global ctrader_connected

    ctrader_connected = False

    print("cTrader disconnected:", reason)


def on_ctrader_error(failure):
    print("cTrader error:", failure)


def on_ctrader_message(client, message):
    global application_authenticated
    global account_authenticated
    global account_id

    try:
        payload = Protobuf.extract(message)

        if isinstance(payload, ProtoOAApplicationAuthRes):

            application_authenticated = True

            print("cTrader application authenticated")

            if access_token is None:
                print("Waiting for OAuth access token...")
                return

            request = ProtoOAGetAccountListByAccessTokenReq()

            request.accessToken = access_token

            client.send(request).addErrback(on_ctrader_error)

        elif isinstance(payload, ProtoOAGetAccountListByAccessTokenRes):

            accounts = payload.ctidTraderAccount

            if not accounts:
                print("No cTrader accounts found")
                return

            selected_account = None

            for account in accounts:
                if not getattr(account, "isLive", True):
                    selected_account = account
                    break

            if selected_account is None:
                selected_account = accounts[0]

            account_id = int(
                selected_account.ctidTraderAccountId
            )

            print("cTrader account found:", account_id)

            request = ProtoOAAccountAuthReq()

            request.ctidTraderAccountId = account_id
            request.accessToken = access_token

            client.send(request).addErrback(on_ctrader_error)

        elif isinstance(payload, ProtoOAAccountAuthRes):

            account_authenticated = True

            account_id = int(
                payload.ctidTraderAccountId
            )

            print(
                "cTrader account authenticated:",
                account_id
            )

            print("cTrader DEMO is READY")

        else:

            print(
                "cTrader message:",
                payload
            )

    except Exception as error:

        print(
            "Error processing cTrader message:",
            error
        )


@app.get("/")
def home():

    return "EURUSD bot online", 200


@app.get("/health")
def health():

    return jsonify({

        "status": "OK",

        "ctrader_configured": bool(
            CLIENT_ID
            and CLIENT_SECRET
            and REDIRECT_URI
        ),

        "authenticated": access_token is not None,

        "ctrader_connected": ctrader_connected,

        "application_authenticated":
            application_authenticated,

        "account_authenticated":
            account_authenticated,

        "account_id": account_id

    }), 200


@app.get("/auth")
def auth():

    if not CLIENT_ID or not REDIRECT_URI:

        return jsonify({
            "error": "cTrader OAuth variables missing"
        }), 500

    url = (
        "https://id.ctrader.com/my/settings/openapi/"
        "grantingaccess/"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=trading"
        "&product=web"
    )

    return redirect(url)


@app.get("/callback")
def callback():

    global access_token
    global refresh_token

    code = request.args.get("code")

    if not code:

        return jsonify({

            "error": "No authorization code received",

            "details":
                request.args.to_dict()

        }), 400

    response = requests.get(

        "https://openapi.ctrader.com/apps/token",

        params={

            "grant_type":
                "authorization_code",

            "code":
                code,

            "redirect_uri":
                REDIRECT_URI,

            "client_id":
                CLIENT_ID,

            "client_secret":
                CLIENT_SECRET

        },

        timeout=20
    )

    data = response.json()

    if (
        response.status_code != 200
        or not data.get("accessToken")
    ):

        return jsonify({

            "error":
                "cTrader token exchange failed",

            "response":
                data

        }), 400

    access_token = data["accessToken"]

    refresh_token = data.get(
        "refreshToken"
    )

    print(
        "OAuth access token received"
    )

    start_ctrader()

    return jsonify({

        "status":
            "cTrader authentication successful",

        "token_received":
            True,

        "message":
            "Connecting to cTrader DEMO..."

    }), 200


@app.post("/webhook")
def webhook():

    if WEBHOOK_SECRET:

        secret = request.headers.get(
            "X-Webhook-Secret"
        )

        if secret != WEBHOOK_SECRET:

            return jsonify({
                "error": "Unauthorized"
            }), 401

    data = request.get_json(
        silent=True
    )

    if not data:

        return jsonify({
            "error": "Invalid JSON"
        }), 400

    action = str(
        data.get("action", "")
    ).upper()

    if action not in ("BUY", "SELL"):

        return jsonify({
            "error": "Invalid action"
        }), 400

    return jsonify({

        "status": "received",

        "action": action,

        "symbol":
            data.get("symbol"),

        "volume":
            data.get("volume"),

        "sl":
            data.get("sl"),

        "tp1":
            data.get("tp1"),

        "tp2":
            data.get("tp2"),

        "ctrader_authenticated":
            account_authenticated

    }), 200


if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
