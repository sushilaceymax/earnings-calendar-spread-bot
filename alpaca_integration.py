import os
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import OptionOrderRequest, OptionOrderLeg, TimeInForce, OrderClass
from alpaca.trading.enums import OptionOrderType, OptionOrderSide
from alpaca.trading.models import Position
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY")
BASE_URL = os.environ.get("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")


def init_alpaca_client():
    try:
        client = TradingClient(API_KEY, API_SECRET, paper=True, base_url=BASE_URL)
        print("Alpaca client initialized.")
        return client
    except Exception as e:
        print(f"Error initializing Alpaca client: {e}")
        return None


def place_calendar_spread_order(symbol, quantity, expiry_short, expiry_long, strike):
    """
    Place a call calendar spread (sell near-term call, buy longer-term call, same strike).
    """
    client = init_alpaca_client()
    if not client:
        return None
    try:
        legs = [
            OptionOrderLeg(
                symbol=symbol,
                expiry=expiry_short,
                strike=strike,
                side=OptionOrderSide.SELL_TO_OPEN,
                type='call',
                quantity=quantity
            ),
            OptionOrderLeg(
                symbol=symbol,
                expiry=expiry_long,
                strike=strike,
                side=OptionOrderSide.BUY_TO_OPEN,
                type='call',
                quantity=quantity
            )
        ]
        order_request = OptionOrderRequest(
            legs=legs,
            order_type=OptionOrderType.DEBIT,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.SIMPLE
        )
        order = client.submit_option_order(order_request)
        print(f"Placed calendar spread order: {order}")
        return order
    except Exception as e:
        print(f"Error placing calendar spread order: {e}")
        return None


def close_calendar_spread_order(symbol, expiry_short, expiry_long, strike, quantity):
    """
    Close both legs of the call calendar spread (buy to close short leg, sell to close long leg).
    """
    client = init_alpaca_client()
    if not client:
        return None
    try:
        legs = [
            OptionOrderLeg(
                symbol=symbol,
                expiry=expiry_short,
                strike=strike,
                side=OptionOrderSide.BUY_TO_CLOSE,
                type='call',
                quantity=quantity
            ),
            OptionOrderLeg(
                symbol=symbol,
                expiry=expiry_long,
                strike=strike,
                side=OptionOrderSide.SELL_TO_CLOSE,
                type='call',
                quantity=quantity
            )
        ]
        order_request = OptionOrderRequest(
            legs=legs,
            order_type=OptionOrderType.CREDIT,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.SIMPLE
        )
        order = client.submit_option_order(order_request)
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