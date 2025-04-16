import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
from automation import compute_recommendation, get_tomorrows_earnings
from alpaca_integration import place_calendar_spread_order, close_calendar_spread_order, get_portfolio_value
import yfinance as yf

load_dotenv()
GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")

# Google Apps Script integration functions

def post_trade(trade_data):
    """POST a new trade to the Google Apps Script endpoint."""
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json=trade_data)
        r.raise_for_status()
        print(f"POST trade: {trade_data} -> {r.text}")
        return r.text
    except Exception as e:
        print(f"Error posting trade: {e}")
        return None

def get_open_trades():
    """GET all 'OPEN' trades from the Google Apps Script endpoint."""
    try:
        r = requests.get(GOOGLE_SCRIPT_URL + "?status=OPEN")
        r.raise_for_status()
        print("Fetched OPEN trades from Google Sheets.")
        return r.json()
    except Exception as e:
        print(f"Error fetching open trades: {e}")
        return []

def update_trade(trade_data):
    """PUT/POST to update a trade as closed in the Google Apps Script endpoint."""
    try:
        r = requests.put(GOOGLE_SCRIPT_URL, json=trade_data)
        r.raise_for_status()
        print(f"Updated trade: {trade_data} -> {r.text}")
        return r.text
    except Exception as e:
        print(f"Error updating trade: {e}")
        return None

def is_time_to_open(earnings_date, when):
    now = datetime.now()
    market_close = time(16, 0)
    if when == "BMO":
        open_dt = datetime.combine(earnings_date - timedelta(days=1), market_close) - timedelta(minutes=15)
    else:  # AMC
        open_dt = datetime.combine(earnings_date, market_close) - timedelta(minutes=15)
    return now >= open_dt and now < open_dt + timedelta(minutes=30)

def is_time_to_close(earnings_date, when):
    now = datetime.now()
    open_time = time(9, 30)
    if when == "BMO":
        close_dt = datetime.combine(earnings_date, open_time) + timedelta(minutes=15)
    else:  # AMC
        close_dt = datetime.combine(earnings_date + timedelta(days=1), open_time) + timedelta(minutes=15)
    return now >= close_dt and now < close_dt + timedelta(minutes=30)

def select_expiries_and_strike(stock, earnings_date):
    """
    Select front and back month expiries and ATM strike for the calendar spread.
    - front: first expiry after earnings_date
    - back: expiry closest to 30 days after front
    - strike: closest to underlying price
    Returns (expiry_short, expiry_long, strike) or (None, None, None) if not found.
    """
    try:
        exp_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in stock.options]
        exp_dates = sorted(exp_dates)
        # Find front month expiry (first after earnings)
        expiry_short = next((d for d in exp_dates if d > earnings_date), None)
        if not expiry_short:
            return None, None, None
        # Find back month expiry (closest to 30 days after front)
        target_back = expiry_short + timedelta(days=30)
        expiry_long = min((d for d in exp_dates if d > expiry_short), key=lambda d: abs((d - target_back).days), default=None)
        if not expiry_long:
            return None, None, None
        # Get ATM strike
        underlying_price = stock.history(period='1d')['Close'].iloc[0]
        chain = stock.option_chain(expiry_short.strftime('%Y-%m-%d'))
        strikes = chain.calls['strike'].tolist()
        strike = min(strikes, key=lambda x: abs(x - underlying_price))
        return expiry_short.strftime('%Y-%m-%d'), expiry_long.strftime('%Y-%m-%d'), strike
    except Exception as e:
        print(f"Error selecting expiries/strike: {e}")
        return None, None, None

def calculate_calendar_spread_cost(stock, expiry_short, expiry_long, strike):
    """
    Calculate the cost of the calendar spread (mid prices).
    Returns total debit per spread (float) or None if not found.
    """
    try:
        chain_short = stock.option_chain(expiry_short)
        chain_long = stock.option_chain(expiry_long)
        call_short = chain_short.calls.loc[chain_short.calls['strike'] == strike]
        call_long = chain_long.calls.loc[chain_long.calls['strike'] == strike]
        if call_short.empty or call_long.empty:
            return None
        # Use mid price (average of bid/ask)
        short_mid = (call_short['bid'].iloc[0] + call_short['ask'].iloc[0]) / 2
        long_mid = (call_long['bid'].iloc[0] + call_long['ask'].iloc[0]) / 2
        cost = long_mid - short_mid
        return float(cost)
    except Exception as e:
        print(f"Error calculating spread cost: {e}")
        return None

def run_trade_workflow():
    print("Running trade workflow...")
    # 1. Close due trades
    open_trades = get_open_trades()
    for trade in open_trades:
        try:
            earnings_date = datetime.strptime(trade['earnings_date'], "%Y-%m-%d").date()
            when = trade.get('when', 'AMC')
            if is_time_to_close(earnings_date, when):
                print(f"Closing trade for {trade['symbol']}...")
                close_calendar_spread_order(
                    trade['symbol'],
                    trade['expiry_short'],
                    trade['expiry_long'],
                    trade['strike'],
                    trade['quantity']
                )
                trade['status'] = 'CLOSED'
                trade['close_time'] = datetime.now().isoformat()
                update_trade(trade)
        except Exception as e:
            print(f"Error closing trade: {e}")
    # 2. Screen and open new trades
    tickers = get_tomorrows_earnings()
    portfolio_value = get_portfolio_value()
    if not portfolio_value:
        print("Could not fetch portfolio value. Skipping trade opening.")
        return
    for ticker in tickers:
        try:
            rec = compute_recommendation(ticker)
            if isinstance(rec, dict) and rec.get('avg_volume') and rec.get('iv30_rv30') and rec.get('ts_slope_0_45'):
                earnings_date = datetime.now().date() + timedelta(days=1)
                when = 'AMC'  # TODO: Replace with actual timing lookup if available
                if is_time_to_open(earnings_date, when):
                    print(f"Preparing trade for {ticker}...")
                    stock = yf.Ticker(ticker)
                    expiry_short, expiry_long, strike = select_expiries_and_strike(stock, earnings_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker}. Skipping.")
                        continue
                    spread_cost = calculate_calendar_spread_cost(stock, expiry_short, expiry_long, strike)
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker}. Skipping.")
                        continue
                    # Kelly position sizing: 10% of portfolio value / spread cost
                    kelly_fraction = 0.10
                    max_allocation = portfolio_value * kelly_fraction
                    quantity = int(max_allocation // (spread_cost * 100))  # 1 contract = 100 shares
                    if quantity < 1:
                        print(f"Kelly sizing yields 0 contracts for {ticker}. Skipping.")
                        continue
                    print(f"Opening trade for {ticker}: {quantity}x {expiry_short}/{expiry_long} @ {strike}, cost/spread: ${spread_cost:.2f}, Kelly allocation: ${max_allocation:.2f}")
                    order = place_calendar_spread_order(ticker, quantity, expiry_short, expiry_long, strike)
                    if order:
                        trade_data = {
                            'symbol': ticker,
                            'expiry_short': expiry_short,
                            'expiry_long': expiry_long,
                            'strike': strike,
                            'quantity': quantity,
                            'earnings_date': earnings_date.strftime('%Y-%m-%d'),
                            'when': when,
                            'status': 'OPEN',
                            'open_time': datetime.now().isoformat(),
                            'kelly_fraction': kelly_fraction,
                            'spread_cost': spread_cost,
                            'portfolio_value': portfolio_value
                        }
                        post_trade(trade_data)
        except Exception as e:
            print(f"Error screening/opening trade for {ticker}: {e}")

if __name__ == "__main__":
    run_trade_workflow() 