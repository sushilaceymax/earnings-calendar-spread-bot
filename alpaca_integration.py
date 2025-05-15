import os
from alpaca.trading.client import TradingClient
from alpaca.trading.models import Position
from dotenv import load_dotenv
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest, OptionLatestQuoteRequest
from alpaca.trading.requests import GetOptionContractsRequest
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest, OptionLegRequest
from alpaca.trading.enums import OrderClass, TimeInForce, OrderSide, PositionIntent
from datetime import datetime, timedelta
import threading, time
from alpaca.trading.requests import GetOrderByIdRequest
from alpaca.trading.enums import OrderStatus
import math
from alpaca.common.exceptions import APIError

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


def place_calendar_spread_order(short_symbol, long_symbol, original_intended_quantity, limit_price=None, on_filled=None, max_total_cost_allowed=None, target_debit_price=None):
    """
    Place a call calendar spread using provided OCC symbols for each leg.
    Optionally specify a limit_price for the spread order (initial target).
    max_total_cost_allowed caps the total debit for the original_intended_quantity.
    target_debit_price is the ideal maximum debit per share; chase won't go significantly beyond this.
    Quantity may be dynamically reduced if price chase makes original quantity too expensive.
    """
    client = init_alpaca_client()
    if not client:
        return None
    
    if original_intended_quantity < 1:
        print(f"Original intended quantity for {short_symbol}/{long_symbol} is < 1. Skipping order.")
        return None

    try:
        # This limit_price parameter is the initial target, but the chase starts from mid_spread
        # We will use it as a reference if provided, but primarily rely on dynamic chasing from mid.
        if limit_price is not None:
            limit_price = round(limit_price, 2) # Store for reference, not directly used to start chase

        # Fetch initial quotes for creeping logic
        short_bid, short_ask, long_bid, long_ask = get_spread_quotes(short_symbol, long_symbol)
        print(f"Initial quotes for {short_symbol}: Bid={short_bid}, Ask={short_ask}")
        print(f"Initial quotes for {long_symbol}: Bid={long_bid}, Ask={long_ask}")
        
        current_market_mid_spread = (long_bid + long_ask) / 2 - (short_bid + short_ask) / 2
        # max_price is the absolute worst price (long ask - short bid)
        market_max_debit = long_ask - short_bid 
        
        price_to_chase = current_market_mid_spread # Start chasing from current mid
        # Dynamic crawling step: half the sum of bid-ask spreads on both legs (min $0.01)
        spread_short = short_ask - short_bid
        spread_long = long_ask - long_bid
        chase_step = max((spread_short + spread_long) / 2.0, 0.01)
        
        # Determine the actual maximum price to chase up to.
        # target_debit_price (ideal entry from initial mid) is for logging/reference here.
        # previously the market_max_debit was used as the effective max chase price
        effective_max_chase_price = long_ask
        if target_debit_price is not None:
            # Log both the ideal target and the actual chase limit (market ask)
            print(f"Ideal target debit price for {short_symbol}/{long_symbol} is ${target_debit_price:.2f}. Will chase up to market ask (effective max chase price): ${effective_max_chase_price:.2f}")
        else:
            # This case might not be hit if trade_workflow always provides target_debit_price
            print(f"No ideal target_debit_price provided. Chasing up to market ask (effective max chase price): ${effective_max_chase_price:.2f}")

        remaining_overall_quantity = original_intended_quantity
        total_cost_so_far = 0.0
        cumulative_filled_order_obj = None # To store the last fill details for the callback
        filled_something_overall = False

        print(f"Starting trade for {short_symbol}/{long_symbol}: original_qty={original_intended_quantity}, max_total_cost_allowed=${max_total_cost_allowed if max_total_cost_allowed is not None else 'N/A'}")

        # Creeping DAY loop from mid toward effective_max_chase_price
        while remaining_overall_quantity > 0 and price_to_chase <= effective_max_chase_price:
            current_limit_price_attempt = round(price_to_chase, 2)
            
            affordable_qty_at_this_lp = remaining_overall_quantity # Default to trying to fill all remaining
            if max_total_cost_allowed is not None:
                remaining_budget = max_total_cost_allowed - total_cost_so_far
                if current_limit_price_attempt <= 0: # Avoid division by zero if spread somehow goes free/credit when chasing debit
                    if remaining_budget < 0: # No budget left even if it's free
                        affordable_qty_at_this_lp = 0
                    # else, if it's free/credit and budget is non-negative, can afford all remaining
                elif remaining_budget > 0:
                    affordable_qty_at_this_lp = math.floor(remaining_budget / (current_limit_price_attempt * 100))
                else: # No budget left
                    affordable_qty_at_this_lp = 0

            qty_for_this_order_attempt = min(remaining_overall_quantity, affordable_qty_at_this_lp)

            if qty_for_this_order_attempt < 1:
                print(f"Stopping chase for {short_symbol}/{long_symbol}: Cannot afford even 1 share/contract at ${current_limit_price_attempt}. Budget left: ${remaining_budget:.2f}, Qty needed: {remaining_overall_quantity}")
                break
            
            # Simplify leg ratios for this specific order attempt's quantity
            # In a 1:1 spread, gcd will be qty_for_this_order_attempt, so ratios are 1
            gcd_val = math.gcd(qty_for_this_order_attempt, qty_for_this_order_attempt)
            ratio_qty_short = qty_for_this_order_attempt // gcd_val
            ratio_qty_long = qty_for_this_order_attempt // gcd_val

            legs_for_this_order = [
                OptionLegRequest(
                    symbol=short_symbol,
                    ratio_qty=ratio_qty_short, # Use ratio for the order quantity
                    side=OrderSide.SELL,
                    position_intent=PositionIntent.SELL_TO_OPEN
                ),
                OptionLegRequest(
                    symbol=long_symbol,
                    ratio_qty=ratio_qty_long, # Use ratio for the order quantity
                    side=OrderSide.BUY,
                    position_intent=PositionIntent.BUY_TO_OPEN
                )
            ]

            req = LimitOrderRequest(
                order_class=OrderClass.MLEG,
                time_in_force=TimeInForce.DAY,
                qty=qty_for_this_order_attempt, # This is the actual Alpaca quantity for this order
                legs=legs_for_this_order,
                limit_price=current_limit_price_attempt
            )
            
            submitted_order_this_attempt = None
            try:
                submitted_order_this_attempt = client.submit_order(req)
                print(f"Placed DAY opening for {short_symbol}/{long_symbol}: {qty_for_this_order_attempt} qty @ limit ${current_limit_price_attempt}. Order ID: {submitted_order_this_attempt.id}")
                
                filled_order_details = wait_for_fill(client, submitted_order_this_attempt.id, timeout=30)
                filled_qty_this_order = int(float(getattr(filled_order_details, 'filled_qty', 0) or 0))
                
                if filled_qty_this_order > 0:
                    filled_something_overall = True
                    avg_fill_price_this_order = float(getattr(filled_order_details, 'filled_avg_price', 0) or 0)
                    cost_this_fill = avg_fill_price_this_order * filled_qty_this_order * 100
                    total_cost_so_far += cost_this_fill
                    remaining_overall_quantity -= filled_qty_this_order
                    cumulative_filled_order_obj = filled_order_details # Update with latest fill details
                    
                    print(f"Order {submitted_order_this_attempt.id} for {short_symbol}/{long_symbol}: Filled {filled_qty_this_order} at ${avg_fill_price_this_order}. Remaining overall: {remaining_overall_quantity}. Total cost so far: ${total_cost_so_far:.2f}")

                    if on_filled and filled_qty_this_order == qty_for_this_order_attempt: # Callback if this attempt fully filled
                         # The callback expects the context of the *entire trade attempt* not just one slice.
                         # For simplicity, we'll call it per filled order that was fully filled for its attempt.
                         # A more complex callback might want cumulative details.
                        on_filled(filled_order_details) 

                if remaining_overall_quantity <= 0:
                    print(f"Entire trade for {short_symbol}/{long_symbol} ({original_intended_quantity} contracts) filled.")
                    break # Exit chase loop
                
                # If the order submitted was partially filled (filled_qty_this_order < qty_for_this_order_attempt)
                # and not fully cancelled by wait_for_fill, try to cancel the remainder before chasing at a new price.
                if filled_qty_this_order < qty_for_this_order_attempt and filled_order_details.status != OrderStatus.CANCELED:
                    try:
                        client.cancel_order_by_id(submitted_order_this_attempt.id)
                        print(f"Cancelled remaining part of order {submitted_order_this_attempt.id} for {short_symbol}/{long_symbol} after partial fill.")
                    except APIError as e:
                        if e.status_code != 422: raise
                        else: print(f"Order {submitted_order_this_attempt.id} not cancelable (likely fully filled/expired); ignoring 422.")
            
            except TimeoutError:
                print(f"Order {submitted_order_this_attempt.id if submitted_order_this_attempt else 'N/A'} ({qty_for_this_order_attempt} qty @ ${current_limit_price_attempt}) for {short_symbol}/{long_symbol} timed out (30s). Cancelling if exists.")
                if submitted_order_this_attempt:
                    try:
                        client.cancel_order_by_id(submitted_order_this_attempt.id)
                    except APIError as e:
                        if e.status_code != 422: raise
                        else: print(f"Order {submitted_order_this_attempt.id} not cancelable on timeout (likely filled/expired); ignoring 422.")
            except Exception as e_order_submission_loop:
                print(f"Error during order submission/fill loop for {short_symbol}/{long_symbol}: {e_order_submission_loop}")
                # Decide if we should break or continue; for now, let's increment price and retry
                # unless it's a critical API error, but most are caught by the outer try/except.
            
            price_to_chase += chase_step
        
        if remaining_overall_quantity > 0 and filled_something_overall:
             print(f"Partially filled trade for {short_symbol}/{long_symbol}. Filled {original_intended_quantity - remaining_overall_quantity} out of {original_intended_quantity}. Cost: ${total_cost_so_far:.2f}")
        elif not filled_something_overall:
            print(f"Could not fill any quantity for {short_symbol}/{long_symbol} within price limits (last attempt price: ${round(price_to_chase-chase_step,2)}, effective max chase: ${effective_max_chase_price:.2f}).")
        
        # Return the details of the last successful fill if any, or None
        # The `on_filled` callback would have been called per successful complete slice.
        # For a final representation, cumulative_filled_order_obj might be useful if the caller expects one Order object. 
        # However, for posting to sheets, each slice might be posted via its own callback firing.
        # Returning the last one for now.
        return cumulative_filled_order_obj 

    except Exception as e:
        print(f"Error placing calendar spread order for {short_symbol}/{long_symbol}: {e}")
        return None


def close_calendar_spread_order(short_symbol, long_symbol, quantity, on_filled=None):
    """
    Close both legs of the call calendar spread using provided OCC symbols.
    """
    client = init_alpaca_client()
    if not client:
        return None
    try:
        # build multi-leg market close order using SDK models
        legs = [
            OptionLegRequest(
                symbol=short_symbol,
                ratio_qty=1,
                side=OrderSide.BUY,
                position_intent=PositionIntent.BUY_TO_CLOSE
            ),
            OptionLegRequest(
                symbol=long_symbol,
                ratio_qty=1,
                side=OrderSide.SELL,
                position_intent=PositionIntent.SELL_TO_CLOSE
            )
        ]
        # Fetch quotes for closing creeping logic
        short_bid, short_ask, long_bid, long_ask = get_spread_quotes(short_symbol, long_symbol)
        mid_spread = (long_bid + long_ask) / 2 - (short_bid + short_ask) / 2
        # previously used the natural market bid (long_bid - short_ask) as floor
        min_price = long_bid
        price = mid_spread
        # dynamic crawling step: half the sum of bid-ask spreads on both legs (min $0.01)
        spread_short = short_ask - short_bid
        spread_long = long_ask - long_bid
        step = max((spread_short + spread_long) / 2.0, 0.01)
        remaining = quantity
        last_order = None
        # Creeping DAY loop from mid down toward floor
        while remaining > 0 and price >= min_price:
            lp = round(price, 2)
            req = LimitOrderRequest(
                order_class=OrderClass.MLEG,
                time_in_force=TimeInForce.DAY,
                qty=remaining,
                legs=legs,
                limit_price=lp
            )
            last_order = client.submit_order(req)
            print(f"Placed DAY closing at limit ${lp}: {last_order}")
            try:
                filled = wait_for_fill(client, last_order.id, timeout=60)
                filled_qty = int(float(getattr(filled, 'filled_qty', 0)))
                remaining -= filled_qty
                if remaining <= 0:
                    break
            except TimeoutError:
                # cancel unfilled order after timeout for DAY TIF
                try:
                    client.cancel_order_by_id(last_order.id)
                except APIError as e:
                    if e.status_code != 422:
                        raise
                    else:
                        print(f"Order {last_order.id} not cancelable (likely filled); ignoring 422.")
            else:
                # cancel any remaining unfilled portion after a fill
                if remaining > 0:
                    try:
                        client.cancel_order_by_id(last_order.id)
                    except APIError as e:
                        if e.status_code != 422:
                            raise
                        else:
                            print(f"Order {last_order.id} not cancelable (likely filled); ignoring 422.")
            price -= step
        return last_order
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
            limit=10000
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


def wait_for_fill(client, order_id, timeout=30, interval=1):
    """
    Poll an order until it is fully filled or timeout expires. Returns the filled order.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        ord = client.get_order_by_id(order_id)
        # Accept partial fills immediately
        if float(getattr(ord, 'filled_qty', 0) or 0) > 0:
            filled_qty_val = float(getattr(ord, 'filled_qty', 0) or 0)
            print(f"wait_for_fill: Order {order_id} filled with qty {filled_qty_val}")
            return ord
        time.sleep(interval)
    raise TimeoutError(f"Order {order_id} not filled in {timeout}s")


def monitor_fill_async(client, order, on_filled, timeout=30, interval=1):
    """
    Start a daemon thread to wait for fill and call on_filled callback when done.
    """
    def _poll():
        try:
            filled = wait_for_fill(client, order.id, timeout=timeout, interval=interval)
            on_filled(filled)
        except Exception as e:
            print(f"Fill monitor error for {order.id}: {e}")
    # spawn and return the thread object for external join
    t = threading.Thread(target=_poll, daemon=True)
    t.start()
    return t


def get_spread_quotes(short_symbol, long_symbol):
    """
    Return bid and ask prices for both option legs.
    """
    options_client = OptionHistoricalDataClient(
        api_key=API_KEY,
        secret_key=API_SECRET
    )
    req = OptionLatestQuoteRequest(symbol_or_symbols=[short_symbol, long_symbol])
    quote_resp = options_client.get_option_latest_quote(req)
    qs = quote_resp.get(short_symbol)
    ql = quote_resp.get(long_symbol)
    if not qs or not ql or None in (qs.bid_price, qs.ask_price, ql.bid_price, ql.ask_price):
        raise RuntimeError(f"Could not fetch bid/ask for {short_symbol} or {long_symbol}")
    return qs.bid_price, qs.ask_price, ql.bid_price, ql.ask_price 