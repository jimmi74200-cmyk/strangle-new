import datetime
import logging
import schedule
import time
from py5paisa import FivePaisaClient
import config
import json
import threading
import csv
from os.path import isfile
import os
import re
import sys
import websockets
import asyncio
import queue

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global State ---
client = FivePaisaClient(cred={
    "APP_NAME": config.APP_NAME,
    "APP_SOURCE": config.APP_SOURCE,
    "USER_ID": config.USER_ID,
    "PASSWORD": config.PASSWORD,
    "USER_KEY": config.USER_KEY,
    "ENCRYPTION_KEY": config.ENCRYPTION_KEY
})
client.set_access_token(config.ACCESS_TOKEN, config.CLIENT_CODE)

ltp_store = {}
action_queue = queue.Queue()
pending_sl_order_ids = []
entry_data = {}
max_pnl = 0
trailing_sl_activated = False
ce_scrip_code = None
pe_scrip_code = None
realized_pnl = 0
trade_is_active = False
ws_manager = None
active_legs = {} # To track which legs are currently open

# --- WebSocket Manager ---
class WebSocketManager:
    def __init__(self):
        self._ws_url = f"wss://openfeed.5paisa.com/feeds/api/chat?Value1={config.ACCESS_TOKEN}|{config.CLIENT_CODE}"
        self._thread = None
        self._subscription_queue = queue.Queue()
        self.is_connected = False

    def _get_initial_subscription_msg(self):
        scrip_info = get_scrip_from_local_file(config.SYMBOL)
        if not scrip_info:
            logging.error(f"Cannot get initial subscription for {config.SYMBOL}.")
            return None
        return json.dumps({
            "Method": "MarketFeedV3", "Operation": "Subscribe", "ClientCode": config.CLIENT_CODE,
            "MarketFeedData": [{"Exch": scrip_info["Exch"], "ExchType": scrip_info["ExchType"], "ScripCode": scrip_info["ScripCode"]}]
        })

    async def _run(self):
        logging.info("Attempting to connect to websocket...")
        try:
            async with websockets.connect(self._ws_url) as websocket:
                self.is_connected = True
                logging.info("WebSocket connected successfully.")
                initial_sub_msg = self._get_initial_subscription_msg()
                if initial_sub_msg:
                    await websocket.send(initial_sub_msg)
                    logging.info(f"Sent initial subscription for {config.SYMBOL}")
                while self.is_connected:
                    try:
                        while not self._subscription_queue.empty():
                            message = self._subscription_queue.get_nowait()
                            await websocket.send(json.dumps(message))
                            logging.info(f"Sent message from queue: {message}")
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        self._on_message(message)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logging.warning("WebSocket connection closed.")
                        break
                    except Exception as e:
                        logging.error(f"Error in websocket run loop: {e}")
                        await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Failed to connect to websocket: {e}")
        finally:
            self.is_connected = False
            logging.info("WebSocket run loop finished.")

    def _on_message(self, message):
        try:
            data_list = json.loads(message)
            for data in data_list:
                if "Token" in data and "LastRate" in data:
                    scrip_code = data["Token"]
                    ltp_store[scrip_code] = data["LastRate"]
                    if trade_is_active and scrip_code in [ce_scrip_code, pe_scrip_code]:
                        check_trade_conditions()
        except Exception as e:
            logging.error(f"Error parsing websocket message: {message} - {e}")

    def start(self):
        self._thread = threading.Thread(target=lambda: asyncio.run(self._run()))
        self._thread.daemon = True
        self._thread.start()

    def subscribe(self, scrips):
        self._subscription_queue.put({
            "Method": "MarketFeedV3", "Operation": "Subscribe", "ClientCode": config.CLIENT_CODE, "MarketFeedData": scrips
        })

    def unsubscribe(self, scrips):
        self._subscription_queue.put({
            "Method": "MarketFeedV3", "Operation": "Unsubscribe", "ClientCode": config.CLIENT_CODE, "MarketFeedData": scrips
        })

# --- Data and Trading Logic ---

def get_nearest_weekly_expiry(symbol):
    try:
        expiry_dates = client.get_expiry("N", symbol)
        if not expiry_dates or 'Expiry' not in expiry_dates:
            logging.error("Could not fetch expiry dates.")
            return None
        today = datetime.date.today()
        nearest_expiry = None
        min_diff = float('inf')
        for expiry in expiry_dates['Expiry']:
            timestamp_str = re.search(r'\d+', expiry['ExpiryDate']).group(0)
            expiry_date = datetime.datetime.fromtimestamp(int(timestamp_str) / 1000).date()
            diff = (expiry_date - today).days
            if 0 <= diff < min_diff:
                min_diff = diff
                nearest_expiry = int(timestamp_str)
        return nearest_expiry
    except Exception as e:
        logging.error(f"Error getting nearest weekly expiry: {e}")
        return None

def get_option_chain(symbol, expiry_date):
    try:
        option_chain = client.get_option_chain("N", symbol, expiry_date)
        if not option_chain or 'Options' not in option_chain or not option_chain['Options']:
            logging.error("Could not fetch option chain or it was empty. API Response: %s", option_chain)
            return None
        return option_chain['Options']
    except Exception as e:
        logging.error(f"Error getting option chain: {e}")
        return None

def get_scrip_from_local_file(symbol):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, 'scrip_data.json')
        with open(file_path, 'r') as f:
            scrip_data = json.load(f)
        return scrip_data.get(symbol)
    except Exception as e:
        logging.error(f"Error reading scrip_data.json: {e}")
        return None

def get_spot_price(symbol):
    try:
        scrip_info = get_scrip_from_local_file(symbol)
        if not scrip_info: return None
        scrip_code = scrip_info["ScripCode"]
        timeout = 10
        start_time = time.time()
        while scrip_code not in ltp_store:
            if time.time() - start_time > timeout:
                logging.error(f"Timed out waiting for spot price for {symbol} (ScripCode: {scrip_code}).")
                return None
            time.sleep(0.1)
        return ltp_store[scrip_code]
    except Exception as e:
        logging.error(f"An error occurred while getting the spot price for {symbol}: {e}")
        return None

def get_atm_strike(spot_price, strikes):
    return min(strikes, key=lambda x: abs(x - spot_price))

def get_strike_interval(strikes, atm_strike):
    try:
        atm_index = strikes.index(atm_strike)
        if atm_index + 1 < len(strikes):
            return strikes[atm_index + 1] - strikes[atm_index]
        elif atm_index > 0:
            return strikes[atm_index] - strikes[atm_index - 1]
    except ValueError:
        pass
    if len(strikes) > 1:
        return strikes[1] - strikes[0]
    logging.warning("Could not determine strike interval. Falling back to default 50.")
    return 50

def get_otm_strikes(atm_strike, strikes, distance):
    strike_diff = get_strike_interval(strikes, atm_strike)
    ce_strike = atm_strike + (distance * strike_diff)
    pe_strike = atm_strike - (distance * strike_diff)
    return ce_strike, pe_strike

def get_itm_strikes(atm_strike, strikes, distance):
    strike_diff = get_strike_interval(strikes, atm_strike)
    ce_strike = atm_strike - (distance * strike_diff)
    pe_strike = atm_strike + (distance * strike_diff)
    return ce_strike, pe_strike

def select_strikes(option_chain, method, premium, spot_price):
    if method == "NEAREST_PREMIUM":
        try:
            ce_strikes = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'CE'}
            pe_strikes = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'PE'}
            if not ce_strikes or not pe_strikes:
                logging.error("Could not find CE or PE strikes in option chain.")
                return None, None
            ce_closest_premium = min(ce_strikes.items(), key=lambda x: abs(x[1] - premium))
            pe_closest_premium = min(pe_strikes.items(), key=lambda x: abs(x[1] - premium))
            return ce_closest_premium[0], pe_closest_premium[0]
        except Exception as e:
            logging.error(f"Error selecting strikes by nearest premium: {e}")
            return None, None
    elif method == "EQUAL_PREMIUM_GAP":
        try:
            strikes = sorted(list(set([o['StrikeRate'] for o in option_chain])))
            if len(strikes) < 2:
                logging.error("Not enough strikes in option chain to determine interval.")
                return None, None
            atm_strike = get_atm_strike(spot_price, strikes)
            strike_interval = get_strike_interval(strikes, atm_strike)
            ce_options = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'CE'}
            pe_options = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'PE'}
            valid_pairs = []
            for i in range(-5, 6):
                put_strike_candidate = atm_strike + (i * strike_interval)
                call_strike_candidate = put_strike_candidate + config.STRANGLE_GAP_POINTS
                if call_strike_candidate in ce_options and put_strike_candidate in pe_options:
                    ce_premium = ce_options[call_strike_candidate]
                    pe_premium = pe_options[put_strike_candidate]
                    premium_diff = abs(ce_premium - pe_premium)
                    valid_pairs.append(((call_strike_candidate, put_strike_candidate), premium_diff))
            if not valid_pairs:
                logging.error("No valid pairs found for the given gap around the ATM.")
                return None, None
            best_pair = min(valid_pairs, key=lambda x: x[1])
            return best_pair[0]
        except Exception as e:
            logging.error(f"Error selecting strikes by equal premium gap: {e}")
            return None, None
    else:
        strikes = sorted(list(set([o['StrikeRate'] for o in option_chain])))
        atm_strike = get_atm_strike(spot_price, strikes)
        if method == "ATM":
            return atm_strike, atm_strike
        elif method == "OTM":
            return get_otm_strikes(atm_strike, strikes, config.STRANGLE_STRIKE_DISTANCE)
        elif method == "ITM":
            return get_itm_strikes(atm_strike, strikes, config.STRANGLE_STRIKE_DISTANCE)
        else:
            logging.error(f"Invalid strike selection method: {method}")
            return None, None

def _place_order_with_retry(order_type, scrip_code, qty, lock, max_retries=3, retry_delay=5, **kwargs):
    """Places an order and retries if it fails, using a lock to ensure thread safety."""
    for i in range(max_retries):
        try:
            # The lock ensures that only one thread can execute client.place_order at a time.
            with lock:
                order_result = client.place_order(
                    OrderType=order_type,
                    Exchange='N',
                    ExchangeType='D',
                    ScripCode=scrip_code,
                    Qty=qty,
                    Price=0, # Market order
                    IsIntraday=True,
                    **kwargs
                )

            # The py5paisa library returns a dictionary.
            # Based on 5paisa docs, 'Status': 0 and 'Message': 'Success' indicate success.
            # However, the library might add its own layers. Let's check the raw message too.
            # A successful placement seems to return a dict with 'Status' == 0.
            # A rejection returns a dict with 'Status' != 0 and a 'Message'.
            if order_result and order_result.get('Status') == 0 and 'Success' in order_result.get('Message', ''):
                logging.info(f"Order placed successfully for ScripCode {scrip_code}. BrokerOrderID: {order_result.get('BrokerOrderID')}")
                return order_result
            else:
                error_message = order_result.get('Message', 'Unknown error')
                logging.warning(f"Order placement failed for ScripCode {scrip_code}. Reason: '{error_message}'. Retrying ({i+1}/{max_retries})...")
                time.sleep(retry_delay)

        except Exception as e:
            logging.error(f"Exception during order placement for ScripCode {scrip_code}: {e}. Retrying ({i+1}/{max_retries})...")
            time.sleep(retry_delay)

    logging.error(f"Failed to place order for ScripCode {scrip_code} after {max_retries} retries.")
    return None

def place_strangle_order():
    global pending_sl_order_ids, entry_data, ce_scrip_code, pe_scrip_code, trade_is_active, active_legs
    if trade_is_active:
        logging.warning("A trade is already active. Skipping new order placement.")
        return
    logging.info("Attempting to place strangle order...")
    nearest_expiry = get_nearest_weekly_expiry(config.SYMBOL)
    if not nearest_expiry: return
    option_chain = get_option_chain(config.SYMBOL, nearest_expiry)
    if not option_chain: return
    spot_price = get_spot_price(config.SYMBOL)
    if not spot_price: return

    logging.info(f"Current {config.SYMBOL} spot price: {spot_price}")
    ce_strike, pe_strike = select_strikes(option_chain, config.STRIKE_SELECTION_METHOD, config.PREMIUM, spot_price)

    if ce_strike and pe_strike:
        logging.info(f"Selected CE strike: {ce_strike}, PE strike: {pe_strike}")
        ce_scrip_code = next((o['ScripCode'] for o in option_chain if o['StrikeRate'] == ce_strike and o['CPType'] == 'CE'), None)
        pe_scrip_code = next((o['ScripCode'] for o in option_chain if o['StrikeRate'] == pe_strike and o['CPType'] == 'PE'), None)

        if ce_scrip_code and pe_scrip_code:
            if config.PAPER_TRADING:
                logging.info(f"[PAPER TRADE] Getting live prices for {ce_strike} CE and {pe_strike} PE to simulate entry.")
                # Subscribe to get the live prices
                ws_manager.subscribe([
                    {"Exch": "N", "ExchType": "D", "ScripCode": ce_scrip_code},
                    {"Exch": "N", "ExchType": "D", "ScripCode": pe_scrip_code}
                ])
                # Wait for prices to arrive
                time.sleep(2) # Brief wait for websocket feed

                ce_entry_price = ltp_store.get(ce_scrip_code)
                pe_entry_price = ltp_store.get(pe_scrip_code)

                if ce_entry_price and pe_entry_price:
                    logging.info(f"[PAPER TRADE] Simulating SELL order at CE Price: {ce_entry_price}, PE Price: {pe_entry_price}")
                    entry_data[ce_scrip_code] = {'strike': ce_strike, 'entry_price': ce_entry_price}
                    entry_data[pe_scrip_code] = {'strike': pe_strike, 'entry_price': pe_entry_price}
                    active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
                    trade_is_active = True
                    logging.info("Paper trade is now active.")
                else:
                    logging.error("Could not fetch live prices for paper trade entry. Halting strategy.")
            else:
                # Place initial orders concurrently using threading to minimize delay
                logging.info(f"Placing strangle orders for {config.SYMBOL} concurrently...")

                results = {}
                order_lock = threading.Lock() # Lock to make client calls thread-safe
                def place_leg(leg_type, scrip_code):
                    """Target function for the thread to place one leg."""
                    results[leg_type] = _place_order_with_retry('S', scrip_code, config.QTY, lock=order_lock)

                ce_thread = threading.Thread(target=place_leg, args=('CE', ce_scrip_code))
                pe_thread = threading.Thread(target=place_leg, args=('PE', pe_scrip_code))

                # Start both threads at nearly the same time
                ce_thread.start()
                pe_thread.start()

                # Wait for both threads to complete before proceeding
                ce_thread.join()
                pe_thread.join()

                ce_order_result = results.get('CE')
                pe_order_result = results.get('PE')

                # --- Handle partial or failed execution ---
                if ce_order_result and pe_order_result:
                    logging.info("Both CE and PE legs placed successfully. Proceeding to confirmation.")

                    # --- Final Confirmation and SL Placement ---
                    for i in range(12): # 60 seconds timeout
                        positions = client.positions()
                        ce_pos = next((p for p in (positions or []) if p['ScripCode'] == ce_scrip_code), None)
                        pe_pos = next((p for p in (positions or []) if p['ScripCode'] == pe_scrip_code), None)

                        if ce_pos and pe_pos:
                            logging.info("Both legs confirmed in positions. Fetching entry prices and placing SL orders.")

                            # Get entry prices
                            ce_entry_price = ce_pos['SellAvgRate']
                            pe_entry_price = pe_pos['SellAvgRate']
                            entry_data[ce_scrip_code] = {'strike': ce_strike, 'entry_price': ce_entry_price}
                            entry_data[pe_scrip_code] = {'strike': pe_strike, 'entry_price': pe_entry_price}
                            logging.info(f"CE Entry: {ce_entry_price}, PE Entry: {pe_entry_price}")

                            # Place SL orders
                            sl_price_ce = ce_entry_price + config.LEG_WISE_SL_POINTS
                            limit_price_ce = sl_price_ce + config.SL_LIMIT_BUFFER
                            sl_ce_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=ce_scrip_code, Qty=abs(ce_pos['NetQty']), Price=limit_price_ce, StopLossPrice=sl_price_ce, IsIntraday=True)
                            if sl_ce_order and sl_ce_order.get('Status') == 0:
                                logging.info(f"Placed SL order for CE leg at {sl_price_ce}.")
                                pending_sl_order_ids.append(sl_ce_order.get('BrokerOrderID'))
                            else:
                                logging.error(f"Failed to place SL order for CE leg. Reason: {sl_ce_order.get('Message') if sl_ce_order else 'Unknown'}")

                            sl_price_pe = pe_entry_price + config.LEG_WISE_SL_POINTS
                            limit_price_pe = sl_price_pe + config.SL_LIMIT_BUFFER
                            sl_pe_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=pe_scrip_code, Qty=abs(pe_pos['NetQty']), Price=limit_price_pe, StopLossPrice=sl_price_pe, IsIntraday=True)
                            if sl_pe_order and sl_pe_order.get('Status') == 0:
                                logging.info(f"Placed SL order for PE leg at {sl_price_pe}.")
                                pending_sl_order_ids.append(sl_pe_order.get('BrokerOrderID'))
                            else:
                                logging.error(f"Failed to place SL order for PE leg. Reason: {sl_pe_order.get('Message') if sl_pe_order else 'Unknown'}")

                            # Subscribe to live feed and activate trade
                            ws_manager.subscribe([
                                {"Exch": "N", "ExchType": "D", "ScripCode": ce_scrip_code},
                                {"Exch": "N", "ExchType": "D", "ScripCode": pe_scrip_code}
                            ])
                            active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
                            trade_is_active = True
                            logging.info("Trade is now active. Monitoring P&L.")
                            return

                        logging.info(f"Waiting for position confirmation... ({i+1}/12)")
                        time.sleep(5)

                    logging.critical("CRITICAL ERROR: Failed to confirm execution of both legs in positions after 60s. Manual check required.")
                    # Attempt to square off any position that might have been created, to be safe.
                    square_off_lock = threading.Lock()
                    positions = client.positions()
                    if any(p['ScripCode'] == ce_scrip_code for p in (positions or [])):
                         logging.warning("Attempting to square off CE leg as a precaution.")
                         _place_order_with_retry('B', ce_scrip_code, config.QTY, lock=square_off_lock, max_retries=3)
                    if any(p['ScripCode'] == pe_scrip_code for p in (positions or [])):
                         logging.warning("Attempting to square off PE leg as a precaution.")
                         _place_order_with_retry('B', pe_scrip_code, config.QTY, lock=square_off_lock, max_retries=3)
                    return # Halt strategy

                elif ce_order_result and not pe_order_result:
                    logging.critical("PE leg failed to place. Squaring off the successful CE leg to avoid a naked position.")
                    _place_order_with_retry('B', ce_scrip_code, config.QTY, lock=threading.Lock(), max_retries=5) # Use more retries for critical square-off
                    logging.error("Strategy halted for the day due to partial execution failure.")
                    return # Stop execution for the day
                elif not ce_order_result and pe_order_result:
                    logging.critical("CE leg failed to place. Squaring off the successful PE leg to avoid a naked position.")
                    _place_order_with_retry('B', pe_scrip_code, config.QTY, lock=threading.Lock(), max_retries=5) # Use more retries for critical square-off
                    logging.error("Strategy halted for the day due to partial execution failure.")
                    return # Stop execution for the day
                else: # Both failed
                    logging.critical("Both CE and PE legs failed to place. Strategy halted for the day.")
                    return # Stop execution for the day
        else:
            logging.error("Could not find scrip codes for selected strikes.")
    else:
        logging.error("Could not select strikes.")

def check_trade_conditions():
    global max_pnl, trailing_sl_activated

    total_pnl = realized_pnl

    # Calculate PNL only for active legs
    if 'CE' in active_legs:
        ce_ltp = ltp_store.get(ce_scrip_code)
        if ce_ltp:
            total_pnl += (entry_data[ce_scrip_code]['entry_price'] - ce_ltp) * config.QTY

    if 'PE' in active_legs:
        pe_ltp = ltp_store.get(pe_scrip_code)
        if pe_ltp:
            total_pnl += (entry_data[pe_scrip_code]['entry_price'] - pe_ltp) * config.QTY

    if not active_legs: # If both legs have been closed individually
        logging.info("Both legs have been closed. Finalizing trade.")
        action_queue.put({'action': 'exit', 'reason': 'BOTH_LEGS_CLOSED'})
        return

    if total_pnl <= config.OVERALL_SL: action_queue.put({'action': 'exit', 'reason': 'OVERALL_SL_HIT'})
    elif total_pnl >= config.OVERALL_TARGET: action_queue.put({'action': 'exit', 'reason': 'OVERALL_TARGET_HIT'})
    elif trailing_sl_activated:
        if total_pnl > max_pnl: max_pnl = total_pnl
        trailing_sl = (int(max_pnl / config.TRAILING_PROFIT_TRIGGER)) * config.TRAILING_PROFIT_LOCKIN
        if total_pnl < trailing_sl: action_queue.put({'action': 'exit', 'reason': 'TRAILING_SL_HIT'})
    elif not trailing_sl_activated and total_pnl >= config.TRAILING_PROFIT_TRIGGER:
        trailing_sl_activated = True
        logging.info("Trailing stop-loss activated.")

def exit_positions(reason="Unknown"):
    global pending_sl_order_ids, entry_data, realized_pnl, ce_scrip_code, pe_scrip_code, trade_is_active, max_pnl, trailing_sl_activated, active_legs
    if not trade_is_active: return
    logging.info(f"Exiting all positions due to: {reason}")

    # Build unsubscribe list from active legs before modifying the dictionary
    scrips_to_unsubscribe = [{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in active_legs.values()]
    if ws_manager and scrips_to_unsubscribe:
        logging.info(f"Unsubscribing from {len(scrips_to_unsubscribe)} scrips.")
        ws_manager.unsubscribe(scrips_to_unsubscribe)

    ce_exit_price = ltp_store.get(ce_scrip_code, 0)
    pe_exit_price = ltp_store.get(pe_scrip_code, 0)

    if not config.PAPER_TRADING:
        # First, cancel any pending SL orders to avoid them executing during our exit.
        if pending_sl_order_ids:
            logging.info(f"Cancelling {len(pending_sl_order_ids)} pending SL order(s)...")
            for order_id in pending_sl_order_ids:
                try:
                    client.cancel_order(order_id)
                    logging.info(f"Successfully cancelled pending SL order: {order_id}")
                except Exception as e:
                    # Log error but continue, as the order might have already executed
                    logging.error(f"Error cancelling SL order {order_id}: {e}. It might have already executed.")
            pending_sl_order_ids = [] # Clear the list after attempting cancellation

        # Now, explicitly square off each active leg using our robust function
        logging.info("Placing market orders to exit active positions.")
        exit_lock = threading.Lock() # Create a lock for exiting
        # Create a copy of items to iterate over, as active_legs might be changed by other threads
        for leg_type, scrip_code in list(active_legs.items()):
            if scrip_code in entry_data:
                logging.info(f"Attempting to square off {leg_type} leg (ScripCode: {scrip_code}).")
                # To square off a short position, we place a simple market buy order
                _place_order_with_retry(
                    order_type='B',
                    scrip_code=scrip_code,
                    qty=config.QTY,
                    lock=exit_lock, # Pass the lock
                    max_retries=5, # Use more retries for critical exits
                    retry_delay=3
                )
        logging.info("All exit order requests placed.")

    final_pnl = realized_pnl
    if ce_scrip_code in entry_data:
        final_pnl += (entry_data[ce_scrip_code]['entry_price'] - ce_exit_price) * config.QTY
    if pe_scrip_code in entry_data:
        final_pnl += (entry_data[pe_scrip_code]['entry_price'] - pe_exit_price) * config.QTY

    log_trade_to_csv({
        'Date': datetime.date.today().isoformat(), 'Symbol': config.SYMBOL, 'EntryTime': config.ENTRY_TIME, 'ExitTime': datetime.datetime.now().strftime("%H:%M:%S"),
        'Call_Strike': entry_data.get(ce_scrip_code, {}).get('strike', 0), 'Put_Strike': entry_data.get(pe_scrip_code, {}).get('strike', 0),
        'Call_Entry_Premium': entry_data.get(ce_scrip_code, {}).get('entry_price', 0), 'Put_Entry_Premium': entry_data.get(pe_scrip_code, {}).get('entry_price', 0),
        'Call_Exit_Price': ce_exit_price, 'Put_Exit_Price': pe_exit_price, 'Final_PnL': final_pnl, 'Exit_Reason': reason, 'Trade_Mode': 'PAPER' if config.PAPER_TRADING else 'LIVE'
    })

    pending_sl_order_ids, entry_data, realized_pnl, max_pnl, active_legs = [], {}, 0, 0, {}
    trade_is_active, trailing_sl_activated = False, False
    ce_scrip_code, pe_scrip_code = None, None
    logging.info("Trade closed and state reset.")

def monitor_positions():
    global active_legs, realized_pnl
    try:
        if not trade_is_active or not active_legs:
            return

        live_positions = client.positions()
        if live_positions is None: # client.positions() can return None on error
            logging.warning("Could not fetch live positions to monitor legs.")
            return

        live_scrip_codes = {p['ScripCode'] for p in live_positions}

        closed_legs = []
        for leg_type, scrip_code in active_legs.items():
            if scrip_code not in live_scrip_codes:
                closed_legs.append(leg_type)

        if closed_legs:
            for leg_type in closed_legs:
                scrip_code = active_legs.pop(leg_type) # Remove from active and get scrip code
                logging.warning(f"Detected that the {leg_type} leg (ScripCode: {scrip_code}) has been closed, likely due to SL hit.")

                # Update realized P&L
                leg_exit_price = ltp_store.get(scrip_code, entry_data[scrip_code]['entry_price']) # Fallback to entry price if LTP not available
                pnl = (entry_data[scrip_code]['entry_price'] - leg_exit_price) * config.QTY
                realized_pnl += pnl
                logging.info(f"Updated realized P&L by {pnl}. New total realized P&L: {realized_pnl}")

                if config.EXIT_STRATEGY_ON_LEG_SL_HIT:
                    logging.info(f"EXIT_ON_LEG_SL_HIT is True. Exiting remaining position.")
                    action_queue.put({'action': 'exit', 'reason': 'LEG_SL_HIT_EXIT'})
                    return # Exit immediately, no need to check other legs

    except Exception as e:
        logging.error(f"Error in position monitor: {e}")


def log_trade_to_csv(trade_data):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'trade_log.csv')
    file_exists = isfile(file_path)
    with open(file_path, 'a', newline='') as csvfile:
        fieldnames = ['Date', 'Symbol', 'EntryTime', 'ExitTime', 'Call_Strike', 'Put_Strike', 'Call_Entry_Premium', 'Put_Entry_Premium', 'Call_Exit_Price', 'Put_Exit_Price', 'Final_PnL', 'Exit_Reason', 'Trade_Mode']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists: writer.writeheader()
        writer.writerow(trade_data)

if __name__ == "__main__":
    logging.info("Starting trading bot...")
    ws_manager = WebSocketManager()
    ws_manager.start()
    logging.info("Waiting for websocket to connect...")
    time.sleep(5)

    if len(sys.argv) > 1 and sys.argv[1] == '--now':
        logging.info("'--now' argument detected. Placing order immediately.")
        place_strangle_order()
        logging.info("Entering monitoring mode for instant trade. Script will exit when trade is closed.")
        last_monitor_time = time.time()
        while trade_is_active:
            if time.time() - last_monitor_time > 5:
                monitor_positions()
                last_monitor_time = time.time()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit': exit_positions(reason=action.get('reason'))
            except queue.Empty: pass
            time.sleep(0.5)
        logging.info("Instant trade session finished. Exiting.")
    else:
        schedule.every().day.at(config.ENTRY_TIME).do(place_strangle_order)
        schedule.every().day.at(config.EXIT_TIME).do(lambda: exit_positions(reason="TIMED_EXIT"))
        schedule.every(5).seconds.do(monitor_positions)
        logging.info(f"Scheduler started. Entry at {config.ENTRY_TIME}, Exit at {config.EXIT_TIME}, Position Monitor every 5s.")
        while True:
            schedule.run_pending()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit': exit_positions(reason=action.get('reason'))
            except queue.Empty: pass
            time.sleep(1)