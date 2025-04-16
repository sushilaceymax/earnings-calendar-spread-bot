import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
from automation import compute_recommendation, get_tomorrows_earnings, get_todays_earnings
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
            earnings_date = datetime.strptime(trade['Open Date'], "%Y-%m-%d").date()
            when = trade.get('When', 'AMC')  # If you have a 'When' column, else default
            if is_time_to_close(earnings_date, when):
                print(f"Closing trade for {trade['Ticker']}...")
                order = close_calendar_spread_order(
                    trade['Ticker'],
                    trade['Expiry Short'],
                    trade['Expiry Long'],
                    trade['Strike'],
                    trade['Size']
                )
                # Try to fetch actual close price and commission from order response
                close_price = 0
                close_comm = 0
                if order and hasattr(order, 'legs'):
                    # Try to sum the fill prices for the legs (if available)
                    try:
                        close_price = sum([float(leg.filled_avg_price or 0) for leg in order.legs])
                        close_comm = getattr(order, 'commission', 0) or 0
                    except Exception:
                        pass
                update_trade({
                    'Ticker': trade['Ticker'],
                    'Open Date': trade['Open Date'],
                    'Close Date': datetime.now().strftime('%Y-%m-%d'),
                    'Close Price': close_price,
                    'Close Comm.': close_comm
                })
        except Exception as e:
            print(f"Error closing trade: {e}")
    # 2. Screen and open new trades
    # Fetch both today's and tomorrow's earnings
    todays_earnings = get_todays_earnings()
    tomorrows_earnings = get_tomorrows_earnings()
    portfolio_value = get_portfolio_value()
    if not portfolio_value:
        print("Could not fetch portfolio value. Skipping trade opening.")
        return
    # Open BMO trades for tomorrow's earnings (open the day before)
    for ticker_info in tomorrows_earnings:
        ticker = ticker_info['act_symbol']
        when = ticker_info.get('when')
        if not when:
            print(f"Skipping {ticker}: no 'when' info available.")
            continue
        when_norm = 'BMO' if 'before' in (when or '').lower() else 'AMC'
        if when_norm != 'BMO':
            continue  # Only process BMO here
        try:
            rec = compute_recommendation(ticker)
            if isinstance(rec, dict) and rec.get('avg_volume') and rec.get('iv30_rv30') and rec.get('ts_slope_0_45'):
                earnings_date = datetime.now().date() + timedelta(days=1)
                if is_time_to_open(earnings_date, when_norm):
                    print(f"Preparing BMO trade for {ticker} ({when_norm})...")
                    stock = yf.Ticker(ticker)
                    expiry_short, expiry_long, strike = select_expiries_and_strike(stock, earnings_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker}. Skipping.")
                        continue
                    spread_cost = calculate_calendar_spread_cost(stock, expiry_short, expiry_long, strike)
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker}. Skipping.")
                        continue
                    kelly_fraction = 0.10
                    max_allocation = portfolio_value * kelly_fraction
                    quantity = int(max_allocation // (spread_cost * 100))  # 1 contract = 100 shares
                    if quantity < 1:
                        print(f"Kelly sizing yields 0 contracts for {ticker}. Skipping.")
                        continue
                    implied_move = rec.get('expected_move', '')
                    print(f"Opening BMO trade for {ticker}: {quantity}x {expiry_short}/{expiry_long} @ {strike}, cost/spread: ${spread_cost:.2f}, Kelly allocation: ${max_allocation:.2f}, Implied Move: {implied_move}")
                    order = place_calendar_spread_order(
                        ticker,
                        quantity,
                        expiry_short,
                        expiry_long,
                        strike
                    )
                    if order is None:
                        print(f"Order placement failed for {ticker}. Skipping posting to Google Sheets.")
                        continue
                    open_price = spread_cost
                    open_comm = 0
                    if hasattr(order, 'legs'):
                        try:
                            open_price = sum([float(getattr(leg, 'filled_avg_price', 0) or 0) for leg in order.legs])
                            open_comm = getattr(order, 'commission', 0) or 0
                        except Exception:
                            pass
                    post_trade({
                        'Ticker': ticker,
                        'Implied Move': implied_move,
                        'Structure': 'Calendar Spread',
                        'Side': 'Long',
                        'Size': quantity,
                        'Open Date': datetime.now().strftime('%Y-%m-%d'),
                        'Open Price': open_price,
                        'Open Comm.': open_comm,
                        'Close Date': '',
                        'Close Price': '',
                        'Close Comm.': ''
                    })
                else:
                    print(f"Skipping {ticker}: not in correct time window to open BMO trade.")
        except Exception as e:
            print(f"Error screening/opening BMO trade for {ticker}: {e}")
    # Open AMC trades for today's earnings (open the day of)
    for ticker_info in todays_earnings:
        ticker = ticker_info['act_symbol']
        when = ticker_info.get('when')
        if not when:
            print(f"Skipping {ticker}: no 'when' info available.")
            continue
        when_norm = 'BMO' if 'before' in (when or '').lower() else 'AMC'
        if when_norm != 'AMC':
            continue  # Only process AMC here
        try:
            rec = compute_recommendation(ticker)
            if isinstance(rec, dict) and rec.get('avg_volume') and rec.get('iv30_rv30') and rec.get('ts_slope_0_45'):
                earnings_date = datetime.now().date()
                if is_time_to_open(earnings_date, when_norm):
                    print(f"Preparing AMC trade for {ticker} ({when_norm})...")
                    stock = yf.Ticker(ticker)
                    expiry_short, expiry_long, strike = select_expiries_and_strike(stock, earnings_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker}. Skipping.")
                        continue
                    spread_cost = calculate_calendar_spread_cost(stock, expiry_short, expiry_long, strike)
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker}. Skipping.")
                        continue
                    kelly_fraction = 0.10
                    max_allocation = portfolio_value * kelly_fraction
                    quantity = int(max_allocation // (spread_cost * 100))  # 1 contract = 100 shares
                    if quantity < 1:
                        print(f"Kelly sizing yields 0 contracts for {ticker}. Skipping.")
                        continue
                    implied_move = rec.get('expected_move', '')
                    print(f"Opening AMC trade for {ticker}: {quantity}x {expiry_short}/{expiry_long} @ {strike}, cost/spread: ${spread_cost:.2f}, Kelly allocation: ${max_allocation:.2f}, Implied Move: {implied_move}")
                    order = place_calendar_spread_order(
                        ticker,
                        quantity,
                        expiry_short,
                        expiry_long,
                        strike
                    )
                    if order is None:
                        print(f"Order placement failed for {ticker}. Skipping posting to Google Sheets.")
                        continue
                    open_price = spread_cost
                    open_comm = 0
                    if hasattr(order, 'legs'):
                        try:
                            open_price = sum([float(getattr(leg, 'filled_avg_price', 0) or 0) for leg in order.legs])
                            open_comm = getattr(order, 'commission', 0) or 0
                        except Exception:
                            pass
                    post_trade({
                        'Ticker': ticker,
                        'Implied Move': implied_move,
                        'Structure': 'Calendar Spread',
                        'Side': 'Long',
                        'Size': quantity,
                        'Open Date': datetime.now().strftime('%Y-%m-%d'),
                        'Open Price': open_price,
                        'Open Comm.': open_comm,
                        'Close Date': '',
                        'Close Price': '',
                        'Close Comm.': ''
                    })
                else:
                    print(f"Skipping {ticker}: not in correct time window to open AMC trade.")
        except Exception as e:
            print(f"Error screening/opening AMC trade for {ticker}: {e}")

if __name__ == "__main__":
    run_trade_workflow() 