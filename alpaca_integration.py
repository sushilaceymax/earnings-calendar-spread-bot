import os
from alpaca.trading.client import TradingClient
from alpaca.trading.models import Position
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY")
PAPER = os.environ.get("ALPACA_PAPER", "true").lower() == "true"


def init_alpaca_client():
    try:
        client = TradingClient(API_KEY, API_SECRET, paper=PAPER)
        print(f"Alpaca client initialized. Paper mode: {PAPER}")
        return client
    except Exception as e:
        print(f"Error initializing Alpaca client: {e}")
        return None


def place_calendar_spread_order(symbol, quantity, expiry_short, expiry_long, strike):
    """
    Place a call calendar spread (sell near-term call, buy longer-term call, same strike) using Alpaca's multi-leg order format.
    """
    client = init_alpaca_client()
    if not client:
        return None
    try:
        # Construct the option symbols according to OCC symbology (e.g., 'AAPL240621C00150000')
        # You may need to adjust this to match your broker's requirements
        def make_option_symbol(symbol, expiry, strike, callput):
            # expiry: 'YYYY-MM-DD' -> 'YYMMDD'
            expiry_fmt = expiry.replace('-', '')[2:]
            # strike: float -> '00000000' (8 digits, 3 decimals, e.g., 150.0 -> '00150000')
            strike_fmt = f"{int(float(strike) * 1000):08d}"
            return f"{symbol.upper()}{expiry_fmt}{callput.upper()}{strike_fmt}"

        call_symbol_short = make_option_symbol(symbol, expiry_short, strike, 'C')
        call_symbol_long = make_option_symbol(symbol, expiry_long, strike, 'C')

        order_data = {
            "order_class": "mleg",
            "time_in_force": "day",
            "order_type": "net_credit",  # or "net_debit" depending on your intent
            "legs": [
                {
                    "side": "sell",
                    "position_intent": "sell_to_open",
                    "symbol": call_symbol_short,
                    "ratio_qty": str(quantity)
                },
                {
                    "side": "buy",
                    "position_intent": "buy_to_open",
                    "symbol": call_symbol_long,
                    "ratio_qty": str(quantity)
                }
            ]
        }
        order = client.submit_order(order_data)
        print(f"Placed calendar spread order: {order}")
        return order
    except Exception as e:
        print(f"Error placing calendar spread order: {e}")
        return None


def close_calendar_spread_order(symbol, expiry_short, expiry_long, strike, quantity):
    """
    Close both legs of the call calendar spread (buy to close short leg, sell to close long leg) using Alpaca's multi-leg order format.
    """
    client = init_alpaca_client()
    if not client:
        return None
    try:
        def make_option_symbol(symbol, expiry, strike, callput):
            expiry_fmt = expiry.replace('-', '')[2:]
            strike_fmt = f"{int(float(strike) * 1000):08d}"
            return f"{symbol.upper()}{expiry_fmt}{callput.upper()}{strike_fmt}"

        call_symbol_short = make_option_symbol(symbol, expiry_short, strike, 'C')
        call_symbol_long = make_option_symbol(symbol, expiry_long, strike, 'C')

        order_data = {
            "order_class": "mleg",
            "time_in_force": "day",
            "order_type": "net_debit",  # or "net_credit" depending on your intent
            "legs": [
                {
                    "side": "buy",
                    "position_intent": "buy_to_close",
                    "symbol": call_symbol_short,
                    "ratio_qty": str(quantity)
                },
                {
                    "side": "sell",
                    "position_intent": "sell_to_close",
                    "symbol": call_symbol_long,
                    "ratio_qty": str(quantity)
                }
            ]
        }
        order = client.submit_order(order_data)
        print(f"Closed calendar spread order: {order}")
        return order
    except Exception as e:
        print(f"Error closing calendar spread order: {e}")
        return None


def get_open_option_positions():
    client = init_alpaca_client()
    if not client:
        return []
    try:
        positions = client.get_all_positions()
        option_positions = [p for p in positions if isinstance(p, Position) and p.asset_class == 'option']
        print(f"Open option positions: {option_positions}")
        return option_positions
    except Exception as e:
        print(f"Error fetching open option positions: {e}")
        return []


def get_portfolio_value():
    """Fetch the current portfolio/account equity value from Alpaca (in USD)."""
    client = init_alpaca_client()
    if not client:
        return None
    try:
        account = client.get_account()
        equity = float(account.equity)
        print(f"Current portfolio value (equity): ${equity}")
        return equity
    except Exception as e:
        print(f"Error fetching portfolio value: {e}")
        return None 