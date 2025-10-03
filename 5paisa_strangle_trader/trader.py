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
current_trailing_sl = 0 # New global to track the TSL level
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
            if order_result and order_result.get('Status') == 0:
                logging.info(f"Order for ScripCode {scrip_code} placed successfully.")
                return order_result
            else:
                logging.warning(f"Order placement failed for {scrip_code}: {order_result.get('Message')}. Retrying...")
                time.sleep(retry_delay)
        except Exception as e:
            logging.error(f"Exception during order placement for {scrip_code}: {e}. Retrying...")
            time.sleep(retry_delay)
    logging.error(f"Failed to place order for {scrip_code} after {max_retries} retries.")
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
                logging.info(f"[PAPER TRADE] Simulating entry for {ce_strike} CE and {pe_strike} PE.")
                entry_data[ce_scrip_code] = {'strike': ce_strike, 'entry_price': 100} # Dummy price
                entry_data[pe_scrip_code] = {'strike': pe_strike, 'entry_price': 100} # Dummy price
                active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
                trade_is_active = True
                logging.info("Paper trade is now active.")
            else:
                logging.info(f"Placing strangle orders for {config.SYMBOL} concurrently...")
                results = {}
                order_lock = threading.Lock()
                def place_leg(leg_type, scrip_code):
                    results[leg_type] = _place_order_with_retry('S', scrip_code, config.QTY, lock=order_lock)

                ce_thread = threading.Thread(target=place_leg, args=('CE', ce_scrip_code))
                pe_thread = threading.Thread(target=place_leg, args=('PE', pe_scrip_code))
                ce_thread.start()
                pe_thread.start()
                ce_thread.join()
                pe_thread.join()
                ce_order_result = results.get('CE')
                pe_order_result = results.get('PE')

                if ce_order_result and pe_order_result:
                    logging.info("Both CE and PE legs placed successfully. Proceeding to confirmation.")
                    for i in range(12):
                        positions = client.positions()
                        ce_pos = next((p for p in (positions or []) if p['ScripCode'] == ce_scrip_code), None)
                        pe_pos = next((p for p in (positions or []) if p['ScripCode'] == pe_scrip_code), None)
                        if ce_pos and pe_pos:
                            logging.info("Both legs confirmed in positions. Fetching entry prices and placing SL orders.")
                            ce_entry_price = ce_pos['SellAvgRate']
                            pe_entry_price = pe_pos['SellAvgRate']
                            entry_data[ce_scrip_code] = {'strike': ce_strike, 'entry_price': ce_entry_price}
                            entry_data[pe_scrip_code] = {'strike': pe_strike, 'entry_price': pe_entry_price}
                            logging.info(f"CE Entry: {ce_entry_price}, PE Entry: {pe_entry_price}")

                            sl_price_ce = ce_entry_price + config.LEG_WISE_SL_POINTS
                            limit_price_ce = sl_price_ce + config.SL_LIMIT_BUFFER
                            sl_ce_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=ce_scrip_code, Qty=abs(ce_pos['NetQty']), Price=limit_price_ce, StopLossPrice=sl_price_ce, IsIntraday=True)
                            if sl_ce_order and sl_ce_order.get('Status') == 0:
                                pending_sl_order_ids.append(sl_ce_order.get('BrokerOrderID'))

                            sl_price_pe = pe_entry_price + config.LEG_WISE_SL_POINTS
                            limit_price_pe = sl_price_pe + config.SL_LIMIT_BUFFER
                            sl_pe_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=pe_scrip_code, Qty=abs(pe_pos['NetQty']), Price=limit_price_pe, StopLossPrice=sl_price_pe, IsIntraday=True)
                            if sl_pe_order and sl_pe_order.get('Status') == 0:
                                pending_sl_order_ids.append(sl_pe_order.get('BrokerOrderID'))

                            ws_manager.subscribe([{"Exch": "N", "ExchType": "D", "ScripCode": ce_scrip_code}, {"Exch": "N", "ExchType": "D", "ScripCode": pe_scrip_code}])
                            active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
                            trade_is_active = True
                            logging.info("Trade is now active. Monitoring P&L.")
                            return
                        logging.info(f"Waiting for position confirmation... ({i+1}/12)")
                        time.sleep(5)
                    logging.critical("CRITICAL ERROR: Failed to confirm execution of both legs in positions after 60s.")
                    square_off_lock = threading.Lock()
                    if any(p['ScripCode'] == ce_scrip_code for p in (client.positions() or [])):
                         _place_order_with_retry('B', ce_scrip_code, config.QTY, lock=square_off_lock, max_retries=3)
                    if any(p['ScripCode'] == pe_scrip_code for p in (client.positions() or [])):
                         _place_order_with_retry('B', pe_scrip_code, config.QTY, lock=square_off_lock, max_retries=3)
                    return
                elif ce_order_result and not pe_order_result:
                    logging.critical("PE leg failed. Squaring off successful CE leg.")
                    _place_order_with_retry('B', ce_scrip_code, config.QTY, lock=threading.Lock(), max_retries=5)
                elif not ce_order_result and pe_order_result:
                    logging.critical("CE leg failed. Squaring off successful PE leg.")
                    _place_order_with_retry('B', pe_scrip_code, config.QTY, lock=threading.Lock(), max_retries=5)
                else:
                    logging.critical("Both CE and PE legs failed to place.")
        else:
            logging.error("Could not find scrip codes for selected strikes.")
    else:
        logging.error("Could not select strikes.")

def get_current_pnl():
    """Calculates the current P&L for all active legs."""
    if not trade_is_active:
        return 0

    current_pnl = realized_pnl
    if 'CE' in active_legs and ce_scrip_code in ltp_store:
        current_pnl += (entry_data[ce_scrip_code]['entry_price'] - ltp_store[ce_scrip_code]) * config.QTY
    if 'PE' in active_legs and pe_scrip_code in ltp_store:
        current_pnl += (entry_data[pe_scrip_code]['entry_price'] - ltp_store[pe_scrip_code]) * config.QTY
    return current_pnl

def check_trade_conditions():
    global max_pnl, trailing_sl_activated, current_trailing_sl
    total_pnl = get_current_pnl()

    if not active_legs:
        logging.info("Both legs have been closed. Finalizing trade.")
        action_queue.put({'action': 'exit', 'reason': 'BOTH_LEGS_CLOSED'})
        return

    if total_pnl <= config.OVERALL_SL: action_queue.put({'action': 'exit', 'reason': 'OVERALL_SL_HIT'})
    elif total_pnl >= config.OVERALL_TARGET: action_queue.put({'action': 'exit', 'reason': 'OVERALL_TARGET_HIT'})
    elif trailing_sl_activated:
        if total_pnl > max_pnl: max_pnl = total_pnl
        trailing_sl = (int(max_pnl / config.TRAILING_PROFIT_TRIGGER)) * config.TRAILING_PROFIT_LOCKIN
        current_trailing_sl = trailing_sl
        if total_pnl < trailing_sl: action_queue.put({'action': 'exit', 'reason': 'TRAILING_SL_HIT'})
    elif not trailing_sl_activated and total_pnl >= config.TRAILING_PROFIT_TRIGGER:
        trailing_sl_activated = True
        max_pnl = total_pnl
        logging.info(f"Trailing stop-loss activated at P&L: {total_pnl:,.2f}")

def exit_positions(reason="Unknown"):
    global pending_sl_order_ids, entry_data, realized_pnl, ce_scrip_code, pe_scrip_code, trade_is_active, max_pnl, trailing_sl_activated, active_legs, current_trailing_sl
    if not trade_is_active: return
    logging.info(f"Exiting all positions due to: {reason}")
    if ws_manager and active_legs:
        ws_manager.unsubscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in active_legs.values()])

    final_pnl = get_current_pnl()

    if not config.PAPER_TRADING:
        if pending_sl_order_ids:
            logging.info(f"Cancelling {len(pending_sl_order_ids)} pending SL order(s)...")
            for order_id in pending_sl_order_ids:
                try: client.cancel_order(order_id)
                except Exception as e: logging.error(f"Error cancelling SL order {order_id}: {e}")

        exit_lock = threading.Lock()
        for leg_type, scrip_code in list(active_legs.items()):
            if scrip_code in entry_data:
                logging.info(f"Placing market order to square off {leg_type} leg.")
                _place_order_with_retry('B', scrip_code, config.QTY, lock=exit_lock, max_retries=5)

    ce_exit_price = ltp_store.get(ce_scrip_code, entry_data.get(ce_scrip_code, {}).get('entry_price', 0))
    pe_exit_price = ltp_store.get(pe_scrip_code, entry_data.get(pe_scrip_code, {}).get('entry_price', 0))
    log_trade_to_csv({
        'Date': datetime.date.today().isoformat(), 'Symbol': config.SYMBOL, 'EntryTime': config.ENTRY_TIME,
        'ExitTime': datetime.datetime.now().strftime("%H:%M:%S"), 'Call_Strike': entry_data.get(ce_scrip_code, {}).get('strike', 0),
        'Put_Strike': entry_data.get(pe_scrip_code, {}).get('strike', 0), 'Call_Entry_Premium': entry_data.get(ce_scrip_code, {}).get('entry_price', 0),
        'Put_Entry_Premium': entry_data.get(pe_scrip_code, {}).get('entry_price', 0), 'Call_Exit_Price': ce_exit_price,
        'Put_Exit_Price': pe_exit_price, 'Final_PnL': final_pnl, 'Exit_Reason': reason, 'Trade_Mode': 'PAPER' if config.PAPER_TRADING else 'LIVE'
    })

    pending_sl_order_ids, entry_data, realized_pnl, max_pnl, active_legs, current_trailing_sl = [], {}, 0, 0, {}, 0
    trade_is_active, trailing_sl_activated = False, False
    ce_scrip_code, pe_scrip_code = None, None
    logging.info("Trade closed and state reset.")

def monitor_positions():
    global active_legs, realized_pnl
    if not trade_is_active or not active_legs: return
    try:
        live_positions = client.positions()
        live_scrip_codes = {p['ScripCode'] for p in (live_positions or [])}
        closed_legs = [leg_type for leg_type, scrip_code in active_legs.items() if scrip_code not in live_scrip_codes]
        if closed_legs:
            for leg_type in closed_legs:
                scrip_code = active_legs.pop(leg_type)
                logging.warning(f"Detected that the {leg_type} leg (ScripCode: {scrip_code}) has been closed, likely due to SL hit.")
                leg_exit_price = ltp_store.get(scrip_code, entry_data[scrip_code]['entry_price'])
                pnl = (entry_data[scrip_code]['entry_price'] - leg_exit_price) * config.QTY
                realized_pnl += pnl
                logging.info(f"Updated realized P&L by {pnl:,.2f}. New total realized P&L: {realized_pnl:,.2f}")
                if config.EXIT_STRATEGY_ON_LEG_SL_HIT:
                    logging.info("Exiting remaining position as per configuration.")
                    action_queue.put({'action': 'exit', 'reason': 'LEG_SL_HIT_EXIT'})
                    return
    except Exception as e:
        logging.error(f"Error in position monitor: {e}")

def log_trade_to_csv(trade_data):
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trade_log.csv')
    file_exists = isfile(file_path)
    with open(file_path, 'a', newline='') as csvfile:
        fieldnames = ['Date', 'Symbol', 'EntryTime', 'ExitTime', 'Call_Strike', 'Put_Strike', 'Call_Entry_Premium', 'Put_Entry_Premium', 'Call_Exit_Price', 'Put_Exit_Price', 'Final_PnL', 'Exit_Reason', 'Trade_Mode']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists: writer.writeheader()
        writer.writerow(trade_data)

def log_pnl_status():
    """Calculates and logs the current P&L and TSL status."""
    if not trade_is_active: return
    current_pnl = get_current_pnl()
    if trailing_sl_activated:
        logging.info(f"P&L: {current_pnl:,.2f} | Max P&L: {max_pnl:,.2f} | Trailing SL: {current_trailing_sl:,.2f}")
    else:
        logging.info(f"P&L: {current_pnl:,.2f}")

if __name__ == "__main__":
    logging.info("Starting trading bot...")
    ws_manager = WebSocketManager()
    ws_manager.start()
    logging.info("Waiting for websocket to connect...")
    time.sleep(5)

    if len(sys.argv) > 1 and sys.argv[1] == '--now':
        logging.info("'--now' argument detected. Placing order immediately.")
        place_strangle_order()
        logging.info("Entering monitoring mode for instant trade.")
        last_monitor_time = time.time()
        last_pnl_log_time = time.time()
        while trade_is_active:
            if time.time() - last_monitor_time > 5:
                monitor_positions()
                last_monitor_time = time.time()
            if time.time() - last_pnl_log_time > 10:
                log_pnl_status()
                last_pnl_log_time = time.time()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit': exit_positions(reason=action.get('reason'))
            except queue.Empty: pass
            time.sleep(0.5)
        logging.info("Instant trade session finished.")
    else:
        schedule.every().day.at(config.ENTRY_TIME).do(place_strangle_order)
        schedule.every().day.at(config.EXIT_TIME).do(lambda: exit_positions(reason="TIMED_EXIT"))
        schedule.every(5).seconds.do(monitor_positions)
        schedule.every(10).seconds.do(log_pnl_status)
        logging.info(f"Scheduler started. Entry at {config.ENTRY_TIME}, Exit at {config.EXIT_TIME}, P&L log every 10s.")
        while True:
            schedule.run_pending()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit': exit_positions(reason=action.get('reason'))
            except queue.Empty: pass
            time.sleep(1)