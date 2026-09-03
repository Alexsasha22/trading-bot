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
    ProtoOASymbolsListReq,
    ProtoOASymbolsListRes,
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOANewOrderReq,
    ProtoOAExecutionEvent,
    ProtoOAOrderErrorEvent,
    ProtoOAAmendPositionSLTPReq,
)

from twisted.internet import reactor

app = Flask(__name__)

CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CTRADER_REDIRECT_URI")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

CTRADER_ENV = os.getenv("CTRADER_ENV", "LIVE").upper()

RISK_PERCENT = 1.0
SL_POINTS = 10
TP_POINTS = 10

access_token = None
refresh_token = None

ctrader_client = None
ctrader_connected = False
application_authenticated = False
account_authenticated = False
account_id = None

account_balance = None
symbols = {}
eurusd_symbol = None

pending_orders = {}


def normalize_symbol(symbol):
    if not symbol:
        return ""

    return (
        str(symbol)
        .upper()
        .replace("/", "")
        .replace("_", "")
        .replace(".", "")
        .replace("-", "")
        .strip()
    )


def find_symbol_id(symbol_name):
    wanted = normalize_symbol(symbol_name)

    if wanted in symbols:
        return symbols[wanted]

    for name, symbol_id in symbols.items():
        if wanted in name or name in wanted:
            return symbol_id

    return None


def send_from_reactor(function, *args):
    if reactor.running:
        reactor.callFromThread(function, *args)
    else:
        print("Twisted reactor is not running")


def start_ctrader():
    global ctrader_client

    if ctrader_client is not None:
        return

    if CTRADER_ENV == "LIVE":
        host = EndPoints.PROTOBUF_LIVE_HOST
        print("Starting cTrader LIVE connection...")
    else:
        host = EndPoints.PROTOBUF_DEMO_HOST
        print("Starting cTrader DEMO connection...")

    ctrader_client = Client(
        host,
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

    print("cTrader connected:", CTRADER_ENV)

    auth_request = ProtoOAApplicationAuthReq()
    auth_request.clientId = CLIENT_ID
    auth_request.clientSecret = CLIENT_SECRET

    deferred = client.send(auth_request)
    deferred.addErrback(on_ctrader_error)


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
    global symbols
    global eurusd_symbol
    global account_balance

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

            return

        if isinstance(payload, ProtoOAGetAccountListByAccessTokenRes):

            accounts = payload.ctidTraderAccount

            if not accounts:
                print("No cTrader accounts found")
                return

            selected_account = None

            wanted_live = CTRADER_ENV == "LIVE"

            for account in accounts:

                is_live = bool(getattr(account, "isLive", False))

                if is_live == wanted_live:
                    selected_account = account
                    break

            if selected_account is None:

                print(
                    "No account matching environment:",
                    CTRADER_ENV
                )

                return

            account_id = int(
                selected_account.ctidTraderAccountId
            )

            print(
                "Selected cTrader account:",
                account_id,
                "LIVE=",
                bool(getattr(selected_account, "isLive", False))
            )

            request = ProtoOAAccountAuthReq()

            request.ctidTraderAccountId = account_id
            request.accessToken = access_token

            client.send(request).addErrback(on_ctrader_error)

            return

        if isinstance(payload, ProtoOAAccountAuthRes):

            account_authenticated = True
            account_id = int(payload.ctidTraderAccountId)

            print(
                "cTrader account authenticated:",
                account_id
            )

            print("Loading account information...")

            request = ProtoOATraderReq()
            request.ctidTraderAccountId = account_id

            client.send(request).addErrback(on_ctrader_error)

            print("Loading cTrader symbols...")

            symbol_request = ProtoOASymbolsListReq()

            symbol_request.ctidTraderAccountId = account_id
            symbol_request.includeArchivedSymbols = False

            client.send(symbol_request).addErrback(on_ctrader_error)

            return

        if isinstance(payload, ProtoOATraderRes):

            trader = payload.trader

            money_digits = int(
                getattr(trader, "moneyDigits", 2)
            )

            account_balance = (
                float(trader.balance)
                / (10 ** money_digits)
            )

            print(
                "Account balance:",
                account_balance,
                "moneyDigits:",
                money_digits
            )

            return

        if isinstance(payload, ProtoOASymbolsListRes):

            symbols = {}

            for symbol in payload.symbol:

                name = normalize_symbol(
                    symbol.symbolName
                )

                if name:

                    symbols[name] = {
                        "id": int(symbol.symbolId),
                        "min_volume": int(
                            getattr(symbol, "minVolume", 0)
                        ),
                        "max_volume": int(
                            getattr(symbol, "maxVolume", 0)
                        ),
                        "step_volume": int(
                            getattr(symbol, "stepVolume", 0)
                        ),
                        "digits": int(
                            getattr(symbol, "digits", 5)
                        )
                    }

            eurusd_symbol = symbols.get("EURUSD")

            if eurusd_symbol:

                print(
                    "EURUSD symbol loaded:",
                    eurusd_symbol
                )

            else:

                print(
                    "WARNING: EURUSD not found"
                )

            print(
                "cTrader",
                CTRADER_ENV,
                "is READY"
            )

            return

        if isinstance(payload, ProtoOAExecutionEvent):

            handle_execution_event(payload)

            return

        if isinstance(payload, ProtoOAOrderErrorEvent):

            print(
                "ORDER ERROR:",
                payload.errorCode,
                getattr(
                    payload,
                    "description",
                    ""
                )
            )

            if getattr(payload, "orderId", 0):

                pending_orders.pop(
                    int(payload.orderId),
                    None
                )

            return

        print("cTrader message:", payload)

    except Exception as error:

        print(
            "Error processing cTrader message:",
            error
        )


def calculate_volume(entry):

    if account_balance is None:
        print("Cannot calculate volume: balance unavailable")
        return None

    if entry <= 0:
        print("Invalid entry:", entry)
        return None

    risk_money = (
        account_balance
        * RISK_PERCENT
        / 100.0
    )

    # EURUSD:
    # 10 points = 0.00010
    distance = SL_POINTS * 0.00001

    # Account currency = EUR
    # EURUSD quote = USD
    # Loss per EUR unit in EUR = distance / entry
    #
    # volume = risk / (distance / entry)

    volume_units = (
        risk_money
        * entry
        / distance
    )

    print(
        "Risk calculation:",
        "balance=", account_balance,
        "risk=", risk_money,
        "entry=", entry,
        "SL distance=", distance,
        "volume=", volume_units
    )

    if eurusd_symbol is None:
        return volume_units

    min_volume = eurusd_symbol["min_volume"]
    max_volume = eurusd_symbol["max_volume"]
    step_volume = eurusd_symbol["step_volume"]

    # cTrader volume values are in 0.01 units.
    # Convert to protocol volume.

    protocol_volume = volume_units * 100

    if min_volume > 0:
        protocol_volume = max(
            protocol_volume,
            min_volume
        )

    if max_volume > 0:
        protocol_volume = min(
            protocol_volume,
            max_volume
        )

    if step_volume > 0:
        protocol_volume = (
            int(protocol_volume / step_volume)
            * step_volume
        )

    final_volume = protocol_volume / 100.0

    print(
        "Final volume:",
        final_volume
    )

    return final_volume


def handle_execution_event(event):

    try:

        execution_type = int(
            event.executionType
        )

        print(
            "cTrader execution event:",
            execution_type
        )

        order = None
        position = None

        if event.HasField("order"):
            order = event.order

        if event.HasField("position"):
            position = event.position

        # Accepted
        if execution_type == 2:

            print("Order accepted by cTrader")

            if order is not None:

                order_id = int(
                    order.orderId
                )

                if order_id in pending_orders:

                    print(
                        "Waiting for order fill:",
                        order_id
                    )

            return

        # Filled / partial fill
        if execution_type in (3, 11):

            print("Order FILLED")

            if order is not None:
                order_id = int(
                    order.orderId
                )
            else:
                order_id = None

            if position is None:

                print(
                    "No position returned with fill."
                )

                return

            position_id = int(
                position.positionId
            )

            pending = None

            if order_id is not None:

                pending = pending_orders.pop(
                    order_id,
                    None
                )

            if pending is None:

                print(
                    "No pending protection found."
                )

                return

            apply_protection(
                position_id,
                pending.get("sl"),
                pending.get("tp")
            )

            return

        # Rejected
        if execution_type == 7:

            print(
                "cTrader order rejected"
            )

            if order is not None:

                pending_orders.pop(
                    int(order.orderId),
                    None
                )

            return

    except Exception as error:

        print(
            "Execution handling error:",
            error
        )


def apply_protection(
    position_id,
    stop_loss,
    take_profit
):

    if not account_authenticated:

        print(
            "Cannot apply protection: "
            "account not authenticated"
        )

        return

    request = ProtoOAAmendPositionSLTPReq()

    request.ctidTraderAccountId = account_id
    request.positionId = int(position_id)

    if stop_loss is not None:

        request.stopLoss = float(
            stop_loss
        )

    if take_profit is not None:

        request.takeProfit = float(
            take_profit
        )

    print(
        "Applying protection:",
        "position=",
        position_id,
        "SL=",
        stop_loss,
        "TP=",
        take_profit
    )

    deferred = ctrader_client.send(
        request
    )

    deferred.addCallbacks(
        on_protection_success,
        on_ctrader_error
    )


def on_protection_success(result):

    print(
        "SL/TP successfully applied"
    )

    return result


def send_market_order(
    action,
    symbol,
    entry
):

    if not ctrader_connected:

        print(
            "Cannot trade: cTrader disconnected"
        )

        return

    if not application_authenticated:

        print(
            "Cannot trade: application "
            "not authenticated"
        )

        return

    if not account_authenticated:

        print(
            "Cannot trade: account "
            "not authenticated"
        )

        return

    symbol_data = symbols.get(
        normalize_symbol(symbol)
    )

    if symbol_data is None:

        print(
            "Symbol not found:",
            symbol
        )

        return

    volume = calculate_volume(
        float(entry)
    )

    if volume is None or volume <= 0:

        print(
            "Invalid calculated volume:",
            volume
        )

        return

    distance = SL_POINTS * 0.00001

    if action == "BUY":

        trade_side = 1

        sl = float(entry) - distance
        tp = float(entry) + (
            TP_POINTS * 0.00001
        )

    else:

        trade_side = 2

        sl = float(entry) + distance
        tp = float(entry) - (
            TP_POINTS * 0.00001
        )

    protocol_volume = int(
        round(volume * 100)
    )

    order = ProtoOANewOrderReq()

    order.ctidTraderAccountId = int(
        account_id
    )

    order.symbolId = int(
        symbol_data["id"]
    )

    order.orderType = 1
    order.tradeSide = trade_side
    order.volume = protocol_volume

    print(
        "Sending",
        CTRADER_ENV,
        "order:",
        action,
        symbol,
        "volume=",
        volume,
        "entry=",
        entry,
        "SL=",
        sl,
        "TP=",
        tp
    )

    deferred = ctrader_client.send(
        order
    )

    def order_response(result):

        try:

            payload = result

            if not isinstance(
                payload,
                ProtoOAExecutionEvent
            ):

                print(
                    "Unexpected order response:",
                    payload
                )

                return result

            if not payload.HasField(
                "order"
            ):

                print(
                    "Execution response "
                    "without order"
                )

                return result

            order_id = int(
                payload.order.orderId
            )

            pending_orders[order_id] = {

                "sl": sl,
                "tp": tp,

                "symbol": symbol,
                "action": action
            }

            print(
                "Order registered:",
                order_id
            )

            if (
                payload.HasField("position")
                and int(
                    payload.executionType
                ) in (3, 11)
            ):

                position_id = int(
                    payload.position.positionId
                )

                pending_orders.pop(
                    order_id,
                    None
                )

                apply_protection(
                    position_id,
                    sl,
                    tp
                )

            return result

        except Exception as error:

            print(
                "Order response error:",
                error
            )

            return result

    deferred.addCallbacks(
        order_response,
        on_ctrader_error
    )


@app.get("/")
def home():

    return (
        "EURUSD bot online",
        200
    )


@app.get("/health")
def health():

    return jsonify({

        "status": "OK",

        "ctrader_environment":
            CTRADER_ENV,

        "ctrader_configured":
            bool(
                CLIENT_ID
                and CLIENT_SECRET
                and REDIRECT_URI
            ),

        "authenticated":
            access_token is not None,

        "ctrader_connected":
            ctrader_connected,

        "application_authenticated":
            application_authenticated,

        "account_authenticated":
            account_authenticated,

        "account_id":
            account_id,

        "account_balance":
            account_balance,

        "eurusd_symbol_loaded":
            "EURUSD" in symbols,

        "risk_percent":
            RISK_PERCENT,

        "sl_points":
            SL_POINTS,

        "tp_points":
            TP_POINTS

    }), 200


@app.get("/auth")
def auth():

    if not CLIENT_ID or not REDIRECT_URI:

        return jsonify({
            "error":
                "cTrader OAuth variables missing"
        }), 500

    url = (
        "https://id.ctrader.com/my/settings/"
        "openapi/grantingaccess/"
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
            "error":
                "No authorization code received",

            "details":
                request.args.to_dict()

        }), 400

    try:

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

    except Exception as error:

        return jsonify({

            "error":
                "Token request failed",

            "details":
                str(error)

        }), 500

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

        "environment":
            CTRADER_ENV,

        "message":
            "Connecting to cTrader..."

    }), 200


@app.post("/webhook")
def webhook():

    if WEBHOOK_SECRET:

        secret = request.headers.get(
            "X-Webhook-Secret"
        )

        if secret != WEBHOOK_SECRET:

            return jsonify({
                "error":
                    "Unauthorized"
            }), 401

    data = request.get_json(
        silent=True
    )

    if not data:

        return jsonify({
            "error":
                "Invalid JSON"
        }), 400

    action = str(
        data.get("action", "")
    ).upper()

    if action not in (
        "BUY",
        "SELL"
    ):

        return jsonify({
            "error":
                "Invalid action"
        }), 400

    symbol = data.get(
        "symbol",
        "EURUSD"
    )

    entry = data.get(
        "entry"
    )

    if entry is None:

        return jsonify({
            "error":
                "Missing entry"
        }), 400

    try:

        entry = float(entry)

    except Exception:

        return jsonify({
            "error":
                "Invalid entry"
        }), 400

    if not account_authenticated:

        return jsonify({

            "status":
                "received",

            "error":
                "cTrader account "
                "not authenticated",

            "ctrader_authenticated":
                False

        }), 503

    if find_symbol_id(symbol) is None:

        return jsonify({

            "status":
                "received",

            "error":
                "Symbol not loaded",

            "symbol":
                symbol

        }), 503

    send_from_reactor(
        send_market_order,
        action,
        symbol,
        entry
    )

    print(
        "Webhook signal received:",
        action,
        symbol,
        entry
    )

    return jsonify({

        "status":
            "order_sent_to_ctrader",

        "action":
            action,

        "symbol":
            symbol,

        "entry":
            entry,

        "risk_percent":
            RISK_PERCENT,

        "sl_points":
            SL_POINTS,

        "tp_points":
            TP_POINTS,

        "ctrader_environment":
            CTRADER_ENV,

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
