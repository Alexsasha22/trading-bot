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
    ProtoOANewOrderReq,
    ProtoOAExecutionEvent,
    ProtoOAOrderErrorEvent,
    ProtoOAAmendPositionSLTPReq,
)

from twisted.internet import reactor


app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CTRADER_REDIRECT_URI")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")


# ============================================================
# GLOBAL STATE
# ============================================================

access_token = None
refresh_token = None

ctrader_client = None

ctrader_connected = False
application_authenticated = False
account_authenticated = False

account_id = None

symbols = {}

pending_orders = {}


# ============================================================
# HELPERS
# ============================================================

def normalize_symbol(symbol):
    """
    Converts symbols such as:

    EURUSD
    EUR/USD
    EURUSD.
    EUR/USD

    into:

    EURUSD
    """

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
    """
    Find the cTrader symbol ID.
    """

    wanted = normalize_symbol(symbol_name)

    if wanted in symbols:
        return symbols[wanted]

    for name, symbol_id in symbols.items():

        if (
            wanted in name
            or name in wanted
        ):
            return symbol_id

    return None


def send_from_reactor(function, *args):
    """
    Safely execute a function inside Twisted's reactor thread.
    """

    if reactor.running:
        reactor.callFromThread(
            function,
            *args
        )
    else:
        print("Twisted reactor is not running")


# ============================================================
# CTRADER CONNECTION
# ============================================================

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

    ctrader_client.setConnectedCallback(
        on_ctrader_connected
    )

    ctrader_client.setDisconnectedCallback(
        on_ctrader_disconnected
    )

    ctrader_client.setMessageReceivedCallback(
        on_ctrader_message
    )

    ctrader_client.startService()

    threading.Thread(
        target=reactor.run,
        kwargs={
            "installSignalHandlers": False
        },
        daemon=True
    ).start()


def on_ctrader_connected(client):

    global ctrader_connected

    ctrader_connected = True

    print("cTrader DEMO connected")

    auth_request = ProtoOAApplicationAuthReq()

    auth_request.clientId = CLIENT_ID
    auth_request.clientSecret = CLIENT_SECRET

    deferred = client.send(
        auth_request
    )

    deferred.addErrback(
        on_ctrader_error
    )


def on_ctrader_disconnected(
    client,
    reason
):

    global ctrader_connected

    ctrader_connected = False

    print(
        "cTrader disconnected:",
        reason
    )


def on_ctrader_error(failure):

    print(
        "cTrader error:",
        failure
    )


# ============================================================
# CTRADER MESSAGE HANDLER
# ============================================================

def on_ctrader_message(
    client,
    message
):

    global application_authenticated
    global account_authenticated
    global account_id
    global symbols

    try:

        payload = Protobuf.extract(
            message
        )

        # ----------------------------------------------------
        # APPLICATION AUTH
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOAApplicationAuthRes
        ):

            application_authenticated = True

            print(
                "cTrader application authenticated"
            )

            if access_token is None:

                print(
                    "Waiting for OAuth access token..."
                )

                return

            request = (
                ProtoOAGetAccountListByAccessTokenReq()
            )

            request.accessToken = access_token

            client.send(
                request
            ).addErrback(
                on_ctrader_error
            )

            return

        # ----------------------------------------------------
        # ACCOUNT LIST
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOAGetAccountListByAccessTokenRes
        ):

            accounts = payload.ctidTraderAccount

            if not accounts:

                print(
                    "No cTrader accounts found"
                )

                return

            selected_account = None

            # Prefer DEMO
            for account in accounts:

                if not getattr(
                    account,
                    "isLive",
                    True
                ):

                    selected_account = account
                    break

            if selected_account is None:

                selected_account = accounts[0]

            account_id = int(
                selected_account.ctidTraderAccountId
            )

            print(
                "cTrader account found:",
                account_id
            )

            request = ProtoOAAccountAuthReq()

            request.ctidTraderAccountId = account_id
            request.accessToken = access_token

            client.send(
                request
            ).addErrback(
                on_ctrader_error
            )

            return

        # ----------------------------------------------------
        # ACCOUNT AUTH
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOAAccountAuthRes
        ):

            account_authenticated = True

            account_id = int(
                payload.ctidTraderAccountId
            )

            print(
                "cTrader account authenticated:",
                account_id
            )

            print(
                "Loading cTrader symbols..."
            )

            request = ProtoOASymbolsListReq()

            request.ctidTraderAccountId = account_id
            request.includeArchivedSymbols = False

            client.send(
                request
            ).addErrback(
                on_ctrader_error
            )

            return

        # ----------------------------------------------------
        # SYMBOL LIST
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOASymbolsListRes
        ):

            symbols = {}

            for symbol in payload.symbol:

                name = normalize_symbol(
                    symbol.symbolName
                )

                if name:

                    symbols[name] = int(
                        symbol.symbolId
                    )

            print(
                "cTrader symbols loaded:",
                len(symbols)
            )

            eurusd_id = find_symbol_id(
                "EURUSD"
            )

            if eurusd_id:

                print(
                    "EURUSD symbol ID:",
                    eurusd_id
                )

            else:

                print(
                    "WARNING: EURUSD not found"
                )

            print(
                "cTrader DEMO is READY"
            )

            return

        # ----------------------------------------------------
        # EXECUTION EVENT
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOAExecutionEvent
        ):

            handle_execution_event(
                payload
            )

            return

        # ----------------------------------------------------
        # ORDER ERROR
        # ----------------------------------------------------

        if isinstance(
            payload,
            ProtoOAOrderErrorEvent
        ):

            print(
                "ORDER ERROR:",
                payload.errorCode,
                getattr(
                    payload,
                    "description",
                    ""
                )
            )

            if getattr(
                payload,
                "orderId",
                0
            ):

                pending_orders.pop(
                    int(payload.orderId),
                    None
                )

            return

        # ----------------------------------------------------
        # OTHER MESSAGE
        # ----------------------------------------------------

        print(
            "cTrader message:",
            payload
        )

    except Exception as error:

        print(
            "Error processing cTrader message:",
            error
        )


# ============================================================
# EXECUTION HANDLING
# ============================================================

def handle_execution_event(
    event
):

    try:

        execution_type = int(
            event.executionType
        )

        print(
            "cTrader execution event:",
            execution_type
        )

        order = None

        if event.HasField(
            "order"
        ):

            order = event.order

        position = None

        if event.HasField(
            "position"
        ):

            position = event.position

        # ----------------------------------------------------
        # ORDER ACCEPTED
        # ----------------------------------------------------

        if execution_type == 2:

            print(
                "Order accepted by cTrader"
            )

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

        # ----------------------------------------------------
        # ORDER FILLED
        # ----------------------------------------------------

        if execution_type in (
            3,
            11
        ):

            print(
                "Order FILLED"
            )

            if order is not None:

                order_id = int(
                    order.orderId
                )

            else:

                order_id = None

            if (
                position is None
                and order_id is not None
            ):

                pending = pending_orders.get(
                    order_id
                )

                if pending:

                    print(
                        "Position not included yet."
                    )

                return

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
                pending.get("tp1")
            )

            return

        # ----------------------------------------------------
        # REJECTED
        # ----------------------------------------------------

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


# ============================================================
# APPLY STOP LOSS / TAKE PROFIT
# ============================================================

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

    if stop_loss is None and take_profit is None:

        print(
            "No SL/TP provided."
        )

        return

    request = ProtoOAAmendPositionSLTPReq()

    request.ctidTraderAccountId = account_id
    request.positionId = int(
        position_id
    )

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


def on_protection_success(
    result
):

    print(
        "SL/TP successfully applied"
    )

    return result


# ============================================================
# SEND MARKET ORDER
# ============================================================

def send_market_order(
    action,
    symbol,
    volume,
    sl,
    tp1
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

    symbol_id = find_symbol_id(
        symbol
    )

    if symbol_id is None:

        print(
            "Symbol not found:",
            symbol
        )

        return

    try:

        # ----------------------------------------------------
        # Volume
        #
        # TradingView sends volume in units.
        #
        # cTrader protocol uses 0.01 unit.
        #
        # Example:
        # volume = 10 units
        # protocol volume = 1000
        # ----------------------------------------------------

        volume_units = float(
            volume
        )

        if volume_units <= 0:

            print(
                "Invalid volume:",
                volume
            )

            return

        protocol_volume = int(
            round(
                volume_units * 100
            )
        )

        # ----------------------------------------------------
        # SIDE
        #
        # BUY = 1
        # SELL = 2
        # ----------------------------------------------------

        if action == "BUY":

            trade_side = 1

        else:

            trade_side = 2

        # ----------------------------------------------------
        # MARKET ORDER
        # ----------------------------------------------------

        order = ProtoOANewOrderReq()

        order.ctidTraderAccountId = int(
            account_id
        )

        order.symbolId = int(
            symbol_id
        )

        order.orderType = 1

        order.tradeSide = trade_side

        order.volume = protocol_volume

        print(
            "Sending DEMO order:",
            action,
            symbol,
            "volume=",
            volume_units
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

                pending_orders[
                    order_id
                ] = {

                    "sl": sl,

                    "tp1": tp1,

                    "symbol": symbol,

                    "action": action

                }

                print(
                    "Order registered:",
                    order_id
                )

                # Sometimes the market order
                # is already filled in this event.

                if (
                    payload.HasField(
                        "position"
                    )
                    and int(
                        payload.executionType
                    ) in (
                        3,
                        11
                    )
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
                        tp1
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

    except Exception as error:

        print(
            "Could not send market order:",
            error
        )


# ============================================================
# HOME
# ============================================================

@app.get("/")
def home():

    return (
        "EURUSD bot online",
        200
    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():

    return jsonify({

        "status":
            "OK",

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

        "eurusd_symbol_loaded":
            find_symbol_id(
                "EURUSD"
            ) is not None

    }), 200


# ============================================================
# OAUTH AUTH
# ============================================================

@app.get("/auth")
def auth():

    if not CLIENT_ID or not REDIRECT_URI:

        return jsonify({

            "error":
                "cTrader OAuth variables missing"

        }), 500

    url = (
        "https://id.ctrader.com/my/settings/openapi/"
        "grantingaccess/"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=trading"
        "&product=web"
    )

    return redirect(
        url
    )


# ============================================================
# OAUTH CALLBACK
# ============================================================

@app.get("/callback")
def callback():

    global access_token
    global refresh_token

    code = request.args.get(
        "code"
    )

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
        or not data.get(
            "accessToken"
        )
    ):

        return jsonify({

            "error":
                "cTrader token exchange failed",

            "response":
                data

        }), 400

    access_token = data[
        "accessToken"
    ]

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


# ============================================================
# WEBHOOK
# ============================================================

@app.post("/webhook")
def webhook():

    # --------------------------------------------------------
    # SECRET
    # --------------------------------------------------------

    if WEBHOOK_SECRET:

        secret = request.headers.get(
            "X-Webhook-Secret"
        )

        if secret != WEBHOOK_SECRET:

            return jsonify({

                "error":
                    "Unauthorized"

            }), 401

    # --------------------------------------------------------
    # JSON
    # --------------------------------------------------------

    data = request.get_json(
        silent=True
    )

    if not data:

        return jsonify({

            "error":
                "Invalid JSON"

        }), 400

    # --------------------------------------------------------
    # ACTION
    # --------------------------------------------------------

    action = str(
        data.get(
            "action",
            ""
        )
    ).upper()

    if action not in (
        "BUY",
        "SELL"
    ):

        return jsonify({

            "error":
                "Invalid action"

        }), 400

    # --------------------------------------------------------
    # SYMBOL
    # --------------------------------------------------------

    symbol = data.get(
        "symbol",
        "EURUSD"
    )

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    volume = data.get(
        "volume"
    )

    if volume is None:

        return jsonify({

            "error":
                "Missing volume"

        }), 400

    # --------------------------------------------------------
    # PROTECTION
    # --------------------------------------------------------

    sl = data.get(
        "sl"
    )

    tp1 = data.get(
        "tp1"
    )

    # --------------------------------------------------------
    # CHECK CTRADER
    # --------------------------------------------------------

    if not account_authenticated:

        return jsonify({

            "status":
                "received",

            "error":
                "cTrader DEMO account is not authenticated",

            "ctrader_authenticated":
                False

        }), 503

    # --------------------------------------------------------
    # CHECK SYMBOL
    # --------------------------------------------------------

    if find_symbol_id(
        symbol
    ) is None:

        return jsonify({

            "status":
                "received",

            "error":
                "Symbol not loaded",

            "symbol":
                symbol

        }), 503

    # --------------------------------------------------------
    # SEND ORDER
    # --------------------------------------------------------

    send_from_reactor(
        send_market_order,
        action,
        symbol,
        volume,
        sl,
        tp1
    )

    print(
        "Webhook signal received:",
        action,
        symbol,
        volume
    )

    return jsonify({

        "status":
            "order_sent_to_ctrader",

        "action":
            action,

        "symbol":
            symbol,

        "volume":
            volume,

        "sl":
            sl,

        "tp1":
            tp1,

        "ctrader_authenticated":
            account_authenticated

    }), 200


# ============================================================
# START SERVER
# ============================================================

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
