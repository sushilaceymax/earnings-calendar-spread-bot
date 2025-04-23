import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
from automation import compute_recommendation, get_tomorrows_earnings, get_todays_earnings
from alpaca_integration import init_alpaca_client, place_calendar_spread_order, close_calendar_spread_order, get_portfolio_value, select_expiries_and_strike_alpaca, get_alpaca_option_chain, get_option_spread_mid_price
import yfinance as yf
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionLatestQuoteRequest
from zoneinfo import ZoneInfo
import sys
import sqlite3

load_dotenv()
GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")
DB_PATH = "trades.db"

# Google Apps Script integration functions

def init_db():
    """Initialize SQLite DB and trades table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS trades (
           "Ticker" TEXT,
           "Implied Move" TEXT,
           "Structure" TEXT,
           "Side" TEXT,
           "Size" INTEGER,
           "Short Symbol" TEXT,
           "Long Symbol" TEXT,
           "Open Date" TEXT,
           "Open Price" REAL,
           "Open Comm." REAL,
           "Close Date" TEXT,
           "Close Price" REAL,
           "Close Comm." REAL
        )'''
    )
    conn.commit()
    conn.close()

init_db()

def post_trade(trade_data):
    """POST a new trade to the Google Apps Script endpoint."""
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json=trade_data)
        r.raise_for_status()
        print(f"POST trade: {trade_data} -> {r.text}")
        # insert into SQLite
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO trades ("Ticker","Implied Move","Structure","Side","Size","Short Symbol","Long Symbol","Open Date","Open Price","Open Comm.","Close Date","Close Price","Close Comm.")
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    trade_data.get('Ticker'),
                    trade_data.get('Implied Move'),
                    trade_data.get('Structure'),
                    trade_data.get('Side'),
                    trade_data.get('Size'),
                    trade_data.get('Short Symbol'),
                    trade_data.get('Long Symbol'),
                    trade_data.get('Open Date'),
                    trade_data.get('Open Price'),
                    trade_data.get('Open Comm.'),
                    trade_data.get('Close Date'),
                    trade_data.get('Close Price'),
                    trade_data.get('Close Comm.')
                )
            )
            conn.commit()
            conn.close()
        except Exception as db_e:
            print(f"Error inserting trade into SQLite: {db_e}")
        return r.text
    except Exception as e:
        print(f"Error posting trade: {e}")
        return None

def get_open_trades():
    """Retrieve open trades from local SQLite DB instead of Google Apps Script."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trades WHERE [Close Date] IS NULL OR [Close Date] = ''")
        rows = cursor.fetchall()
        col_names = [description[0] for description in cursor.description]
        trades = [dict(zip(col_names, row)) for row in rows]
        conn.close()
        print("Fetched open trades from SQLite DB.")
        return trades
    except Exception as e:
        print(f"Error fetching open trades from SQLite DB: {e}")
        return []

def update_trade(trade_data):
    """PUT/POST to update a trade as closed in the Google Apps Script endpoint."""
    try:
        r = requests.put(GOOGLE_SCRIPT_URL, json=trade_data)
        r.raise_for_status()
        print(f"Updated trade: {trade_data} -> {r.text}")
        # update SQLite
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(
                """UPDATE trades
                   SET "Close Date" = ?,
                       "Close Price" = ?,
                       "Close Comm." = ?
                   WHERE "Ticker" = ? AND "Open Date" = ?""",
                (
                    trade_data.get('Close Date'),
                    trade_data.get('Close Price'),
                    trade_data.get('Close Comm.'),
                    trade_data.get('Ticker'),
                    trade_data.get('Open Date')
                )
            )
            conn.commit()
            conn.close()
        except Exception as db_e:
            print(f"Error updating trade in SQLite: {db_e}")
        return r.text
    except Exception as e:
        print(f"Error updating trade: {e}")
        return None

def is_time_to_open(earnings_date, when):
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(tz=eastern)
    market_close = time(16, 0)
    if when == "BMO":
        open_dt = datetime.combine(earnings_date - timedelta(days=1), market_close, tzinfo=eastern) - timedelta(minutes=15)
    else:  # AMC
        open_dt = datetime.combine(earnings_date, market_close, tzinfo=eastern) - timedelta(minutes=15)
    return open_dt <= now < open_dt + timedelta(minutes=30)

def is_time_to_close(earnings_date, when):
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(tz=eastern)
    open_time = time(9, 30)
    if when == "BMO":
        close_dt = datetime.combine(earnings_date, open_time, tzinfo=eastern) + timedelta(minutes=15)
    else:  # AMC
        close_dt = datetime.combine(earnings_date + timedelta(days=1), open_time, tzinfo=eastern) + timedelta(minutes=15)
    return close_dt <= now < close_dt + timedelta(minutes=30)

def select_expiries_and_strike_yahoo(stock, earnings_date):
    """
    (Renamed) Select front and back month expiries and ATM strike for the calendar spread using Yahoo Finance.
    """
    try:
        exp_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in stock.options]
        exp_dates = sorted(exp_dates)
        expiry_short = next((d for d in exp_dates if d > earnings_date), None)
        if not expiry_short:
            return None, None, None
        target_back = expiry_short + timedelta(days=30)
        expiry_long = min((d for d in exp_dates if d > expiry_short), key=lambda d: abs((d - target_back).days), default=None)
        if not expiry_long:
            return None, None, None
        underlying_price = stock.history(period='1d')['Close'].iloc[0]
        chain = stock.option_chain(expiry_short.strftime('%Y-%m-%d'))
        strikes = chain.calls['strike'].tolist()
        strike = min(strikes, key=lambda x: abs(x - underlying_price))
        return expiry_short.strftime('%Y-%m-%d'), expiry_long.strftime('%Y-%m-%d'), strike
    except Exception as e:
        print(f"Error selecting expiries/strike: {e}")
        return None, None, None

def calculate_calendar_spread_cost_yahoo(stock, expiry_short, expiry_long, strike):
    """
    (Renamed) Calculate the cost of the calendar spread (mid prices) using Yahoo Finance.
    """
    try:
        chain_short = stock.option_chain(expiry_short)
        chain_long = stock.option_chain(expiry_long)
        call_short = chain_short.calls.loc[chain_short.calls['strike'] == strike]
        call_long = chain_long.calls.loc[chain_long.calls['strike'] == strike]
        if call_short.empty or call_long.empty:
            return None
        short_mid = (call_short['bid'].iloc[0] + call_short['ask'].iloc[0]) / 2
        long_mid = (call_long['bid'].iloc[0] + call_long['ask'].iloc[0]) / 2
        cost = long_mid - short_mid
        return float(cost)
    except Exception as e:
        print(f"Error calculating spread cost: {e}")
        return None

def run_trade_workflow():
    print("Running trade workflow...")
    # 0. Market open check via Alpaca clock
    client = init_alpaca_client()
    if not client:
        print("Could not initialize Alpaca client. Exiting.")
        return
    clock = client.get_clock()
    if not getattr(clock, 'is_open', False):
        print(f"Market is closed (next open at {clock.next_open}). Exiting.")
        return 1
    print(f"Market is open (current time: {clock.timestamp}). Continuing...")
    # 1. Close due trades
    open_trades = get_open_trades()
    for trade in open_trades:
        try:
            earnings_date = datetime.strptime(trade['Open Date'], "%Y-%m-%d").date()
            when = trade.get('When', 'AMC')  # If you have a 'When' column, else default
            if is_time_to_close(earnings_date, when):
                print(f"Closing trade for {trade['Ticker']}...")
                # use OCC symbols captured in DB
                order = close_calendar_spread_order(
                    trade.get('Short Symbol'),
                    trade.get('Long Symbol'),
                    trade.get('Size')
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
                    # allow same-day expiry for BMO by filtering from one day earlier
                    filter_date = earnings_date - timedelta(days=1) if when_norm == 'BMO' else earnings_date
                    expiry_short, expiry_long, strike = select_expiries_and_strike_alpaca(ticker, filter_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker} using Alpaca. Trying Yahoo...")
                        stock = yf.Ticker(ticker)
                        expiry_short, expiry_long, strike = select_expiries_and_strike_yahoo(stock, filter_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker}. Skipping.")
                        continue
                    spread_cost = get_option_spread_mid_price(ticker, expiry_short, expiry_long, strike)
                    print(f"Alpaca spread_cost for {ticker}: {spread_cost}")
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker} using Alpaca (value={spread_cost}). Trying Yahoo...")
                        stock = yf.Ticker(ticker)
                        spread_cost = calculate_calendar_spread_cost_yahoo(stock, expiry_short, expiry_long, strike)
                        print(f"Yahoo spread_cost for {ticker}: {spread_cost}")
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker} (value={spread_cost}). Skipping.")
                        continue
                    # Fetch OCC symbols from Alpaca chain
                    chain = get_alpaca_option_chain(ticker)
                    short_contract = chain.get(expiry_short, {}).get(strike, {}).get('call')
                    long_contract = chain.get(expiry_long, {}).get(strike, {}).get('call')
                    short_symbol = getattr(short_contract, 'symbol', None)
                    long_symbol = getattr(long_contract, 'symbol', None)
                    # Fetch live mid price for limit order
                    limit_price = get_option_spread_mid_price(ticker, expiry_short, expiry_long, strike)
                    kelly_fraction = 0.10
                    max_allocation = portfolio_value * kelly_fraction
                    quantity = int(max_allocation // (spread_cost * 100))  # 1 contract = 100 shares
                    if quantity < 1:
                        print(f"Kelly sizing yields 0 contracts for {ticker}. Skipping.")
                        continue
                    implied_move = rec.get('expected_move', '')
                    print(f"Opening BMO trade for {ticker}: {quantity}x {expiry_short}/{expiry_long} @ {strike}, cost/spread: ${spread_cost:.2f}, Kelly allocation: ${max_allocation:.2f}, Implied Move: {implied_move}")
                    order = place_calendar_spread_order(
                        short_symbol,
                        long_symbol,
                        quantity,
                        limit_price=limit_price
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
                        'Short Symbol': short_symbol,
                        'Long Symbol': long_symbol,
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
                    expiry_short, expiry_long, strike = select_expiries_and_strike_alpaca(ticker, earnings_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker} using Alpaca. Trying Yahoo...")
                        stock = yf.Ticker(ticker)
                        expiry_short, expiry_long, strike = select_expiries_and_strike_yahoo(stock, earnings_date)
                    if not expiry_short or not expiry_long or not strike:
                        print(f"Could not determine expiries/strike for {ticker}. Skipping.")
                        continue
                    spread_cost = get_option_spread_mid_price(ticker, expiry_short, expiry_long, strike)
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker} using Alpaca. Trying Yahoo...")
                        stock = yf.Ticker(ticker)
                        spread_cost = calculate_calendar_spread_cost_yahoo(stock, expiry_short, expiry_long, strike)
                    if not spread_cost or spread_cost <= 0:
                        print(f"Invalid spread cost for {ticker}. Skipping.")
                        continue
                    # Fetch OCC symbols from Alpaca chain for AMC
                    chain = get_alpaca_option_chain(ticker)
                    short_contract = chain.get(expiry_short, {}).get(strike, {}).get('call')
                    long_contract = chain.get(expiry_long, {}).get(strike, {}).get('call')
                    short_symbol = getattr(short_contract, 'symbol', None)
                    long_symbol = getattr(long_contract, 'symbol', None)
                    limit_price = get_option_spread_mid_price(ticker, expiry_short, expiry_long, strike)
                    kelly_fraction = 0.10
                    max_allocation = portfolio_value * kelly_fraction
                    quantity = int(max_allocation // (spread_cost * 100))  # 1 contract = 100 shares
                    if quantity < 1:
                        print(f"Kelly sizing yields 0 contracts for {ticker}. Skipping.")
                        continue
                    implied_move = rec.get('expected_move', '')
                    print(f"Opening AMC trade for {ticker}: {quantity}x {expiry_short}/{expiry_long} @ {strike}, cost/spread: ${spread_cost:.2f}, Kelly allocation: ${max_allocation:.2f}, Implied Move: {implied_move}")
                    order = place_calendar_spread_order(
                        short_symbol,
                        long_symbol,
                        quantity,
                        limit_price=limit_price
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
                        'Short Symbol': short_symbol,
                        'Long Symbol': long_symbol,
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
    sys.exit(run_trade_workflow())