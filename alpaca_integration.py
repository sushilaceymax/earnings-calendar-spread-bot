import os
from alpaca.trading.client import TradingClient
from alpaca.trading.models import Position
from dotenv import load_dotenv
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest, OptionLatestQuoteRequest
from alpaca.trading.requests import GetOptionContractsRequest
from datetime import datetime, timedelta

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


def place_calendar_spread_order(symbol, quantity, expiry_short, expiry_long, strike, limit_price=None):
    """
    Place a call calendar spread (sell near-term call, buy longer-term call, same strike) using Alpaca's multi-leg order format.
    Optionally specify a limit_price for the spread order.
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
        if limit_price is not None:
            order_data["limit_price"] = limit_price
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


def get_alpaca_option_chain(symbol):
    """
    Fetch the option chain for a given symbol using Alpaca's REST API.
    Returns a dict: {expiry: {strike: {call: {...}, put: {...}}}}
    """
    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import GetOptionContractsRequest
        from datetime import datetime
        trading_client = TradingClient(API_KEY, API_SECRET, paper=PAPER)
        today = datetime.now().date()
        req = GetOptionContractsRequest(
            underlying_symbols=[symbol.upper()],
            expiration_date_gte=today,
            limit=1000
        )
        response = trading_client.get_option_contracts(req)
        contracts = response.option_contracts or []
        # Organize by expiry and strike
        option_chain = {}
        for contract in contracts:
            expiry = contract.expiration_date.strftime('%Y-%m-%d')
            strike = float(contract.strike_price)
            cp = contract.type  # 'call' or 'put'
            if expiry not in option_chain:
                option_chain[expiry] = {}
            if strike not in option_chain[expiry]:
                option_chain[expiry][strike] = {}
            option_chain[expiry][strike][cp] = contract
        return option_chain
    except Exception as e:
        print(f"Error fetching Alpaca option chain for {symbol}: {e}")
        return None


def select_expiries_and_strike_alpaca(symbol, earnings_date):
    """
    Use Alpaca's option chain to select front and back month expiries and ATM strike for the calendar spread.
    Returns (expiry_short, expiry_long, strike) or (None, None, None) if not found.
    """
    option_chain = get_alpaca_option_chain(symbol)
    if not option_chain:
        return None, None, None
    try:
        exp_dates = sorted([datetime.strptime(d, "%Y-%m-%d").date() for d in option_chain.keys()])
        # Find front month expiry (first after earnings)
        expiry_short = next((d for d in exp_dates if d > earnings_date), None)
        if not expiry_short:
            return None, None, None
        # Find back month expiry (closest to 30 days after front)
        target_back = expiry_short + timedelta(days=30)
        expiry_long = min((d for d in exp_dates if d > expiry_short), key=lambda d: abs((d - target_back).days), default=None)
        if not expiry_long:
            return None, None, None
        # Get ATM strike (closest to underlying price)
        # Fetch underlying price from Alpaca (latest bar)
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockLatestBarRequest
        stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)
        bar_resp = stock_client.get_stock_latest_bar(StockLatestBarRequest(symbol_or_symbols=symbol))
        if not bar_resp or symbol.upper() not in bar_resp:
            print(f"No price data for {symbol}")
            return None, None, None
        underlying_price = bar_resp[symbol.upper()].close
        strikes = list(option_chain[expiry_short.strftime('%Y-%m-%d')].keys())
        strike = min(strikes, key=lambda x: abs(x - underlying_price))
        return expiry_short.strftime('%Y-%m-%d'), expiry_long.strftime('%Y-%m-%d'), strike
    except Exception as e:
        print(f"Error selecting expiries/strike from Alpaca: {e}")
        return None, None, None


def get_option_spread_mid_price(symbol, expiry_short, expiry_long, strike, callput='C'):
    """
    Fetch the latest quotes for both legs and return the mid price for the calendar spread (long_mid - short_mid).
    Returns float or None if unavailable.
    """
    def make_option_symbol(symbol, expiry, strike, callput):
        expiry_fmt = expiry.replace('-', '')[2:]
        strike_fmt = f"{int(float(strike) * 1000):08d}"
        return f"{symbol.upper()}{expiry_fmt}{callput.upper()}{strike_fmt}"
    try:
        options_client = OptionHistoricalDataClient(
            api_key=os.environ.get("APCA_API_KEY_ID"),
            secret_key=os.environ.get("APCA_API_SECRET_KEY")
        )
        call_symbol_short = make_option_symbol(symbol, expiry_short, strike, 'C')
        call_symbol_long = make_option_symbol(symbol, expiry_long, strike, 'C')
        req = OptionLatestQuoteRequest(symbol_or_symbols=[call_symbol_short, call_symbol_long])
        quote_resp = options_client.get_option_latest_quote(req)
        quote_short = quote_resp.get(call_symbol_short)
        quote_long = quote_resp.get(call_symbol_long)
        if not quote_short or not quote_long:
            return None
        short_bid = quote_short.bid_price
        short_ask = quote_short.ask_price
        long_bid = quote_long.bid_price
        long_ask = quote_long.ask_price
        if None in (short_bid, short_ask, long_bid, long_ask):
            return None
        short_mid = (short_bid + short_ask) / 2
        long_mid = (long_bid + long_ask) / 2
        return float(long_mid - short_mid)
    except Exception as e:
        print(f"Error fetching Alpaca spread mid price: {e}")
        return None 