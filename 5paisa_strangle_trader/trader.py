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
current_trailing_sl = 0
ce_scrip_code = None
pe_scrip_code = None
realized_pnl = 0
trade_is_active = False
ws_manager = None
active_legs = {}

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
                while self.is_connected:
                    try:
                        while not self._subscription_queue.empty():
                            message = self._subscription_queue.get_nowait()
                            await websocket.send(json.dumps(message))
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        self._on_message(message)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        break
                    except Exception as e:
                        logging.error(f"Error in websocket run loop: {e}")
                        await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Failed to connect to websocket: {e}")
        finally:
            self.is_connected = False

    def _on_message(self, message):
        try:
            data_list = json.loads(message)
            for data in data_list:
                if "Token" in data and "LastRate" in data:
                    ltp_store[data["Token"]] = data["LastRate"]
                    if trade_is_active and data["Token"] in [ce_scrip_code, pe_scrip_code]:
                        check_trade_conditions()
        except Exception as e:
            logging.error(f"Error parsing websocket message: {message} - {e}")

    def start(self):
        self._thread = threading.Thread(target=lambda: asyncio.run(self._run()))
        self._thread.daemon = True
        self._thread.start()

    def subscribe(self, scrips):
        self._subscription_queue.put({"Method": "MarketFeedV3", "Operation": "Subscribe", "ClientCode": config.CLIENT_CODE, "MarketFeedData": scrips})

    def unsubscribe(self, scrips):
        self._subscription_queue.put({"Method": "MarketFeedV3", "Operation": "Unsubscribe", "ClientCode": config.CLIENT_CODE, "MarketFeedData": scrips})

# --- Trading Logic ---
def get_nearest_weekly_expiry(symbol):
    try:
        expiry_dates = client.get_expiry("N", symbol)
        if not expiry_dates or 'Expiry' not in expiry_dates: return None
        today = datetime.date.today()
        nearest_expiry = min((d for d in expiry_dates['Expiry'] if (datetime.datetime.fromtimestamp(int(re.search(r'\d+', d['ExpiryDate']).group(0)) / 1000).date() - today).days >= 0), key=lambda d: (datetime.datetime.fromtimestamp(int(re.search(r'\d+', d['ExpiryDate']).group(0)) / 1000).date() - today).days)
        return int(re.search(r'\d+', nearest_expiry['ExpiryDate']).group(0))
    except Exception as e:
        logging.error(f"Error getting nearest weekly expiry: {e}")
        return None

def get_option_chain(symbol, expiry_date):
    try:
        option_chain = client.get_option_chain("N", symbol, expiry_date)
        return option_chain.get('Options')
    except Exception as e:
        logging.error(f"Error getting option chain: {e}")
        return None

def get_scrip_from_local_file(symbol):
    try:
        with open(os.path.join(os.path.dirname(__file__), 'scrip_data.json'), 'r') as f:
            return json.load(f).get(symbol)
    except Exception as e:
        logging.error(f"Error reading scrip_data.json: {e}")
        return None

def get_spot_price(symbol):
    scrip_info = get_scrip_from_local_file(symbol)
    if not scrip_info: return None
    scrip_code = scrip_info["ScripCode"]
    timeout = 10
    start_time = time.time()
    while scrip_code not in ltp_store:
        if time.time() - start_time > timeout:
            logging.error(f"Timed out waiting for spot price for {symbol}.")
            return None
        time.sleep(0.1)
    return ltp_store[scrip_code]

def select_strikes(option_chain, spot_price):
    strikes = sorted(list(set([o['StrikeRate'] for o in option_chain])))
    atm_strike = min(strikes, key=lambda x: abs(x - spot_price))

    method = config.STRIKE_SELECTION_METHOD
    if method == "ATM":
        return atm_strike, atm_strike

    strike_interval = strikes[1] - strikes[0] if len(strikes) > 1 else 50

    if method == "OTM":
        return atm_strike + (config.STRANGLE_STRIKE_DISTANCE * strike_interval), atm_strike - (config.STRANGLE_STRIKE_DISTANCE * strike_interval)
    if method == "ITM":
        return atm_strike - (config.STRANGLE_STRIKE_DISTANCE * strike_interval), atm_strike + (config.STRANGLE_STRIKE_DISTANCE * strike_interval)

    if method == "NEAREST_PREMIUM":
        ce_strikes = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'CE' and o['StrikeRate'] >= atm_strike}
        pe_strikes = {o['StrikeRate']: o['LastRate'] for o in option_chain if o['CPType'] == 'PE' and o['StrikeRate'] <= atm_strike}
        if not ce_strikes or not pe_strikes: return None, None
        ce_closest = min(ce_strikes.items(), key=lambda x: abs(x[1] - config.PREMIUM))
        pe_closest = min(pe_strikes.items(), key=lambda x: abs(x[1] - config.PREMIUM))
        return ce_closest[0], pe_closest[0]

    if method == "EQUAL_PREMIUM_GAP":
        options = {o['StrikeRate']: o for o in option_chain}
        valid_pairs = []
        for i in range(-5, 6):
            pe_strike = atm_strike + (i * strike_interval)
            ce_strike = pe_strike + config.STRANGLE_GAP_POINTS
            if ce_strike in options and pe_strike in options and options[ce_strike]['CPType'] == 'CE' and options[pe_strike]['CPType'] == 'PE':
                premium_diff = abs(options[ce_strike]['LastRate'] - options[pe_strike]['LastRate'])
                valid_pairs.append(((ce_strike, pe_strike), premium_diff))
        if not valid_pairs: return None, None
        return min(valid_pairs, key=lambda x: x[1])[0]

    return None, None

def _place_order_with_retry(order_type, scrip_code, qty, lock, **kwargs):
    for i in range(3):
        try:
            with lock:
                order_result = client.place_order(OrderType=order_type, Exchange='N', ExchangeType='D', ScripCode=scrip_code, Qty=qty, Price=0, IsIntraday=True, **kwargs)
            if order_result and order_result.get('Status') == 0:
                return order_result
            logging.warning(f"Order failed for {scrip_code}: {order_result.get('Message')}. Retry {i+1}/3.")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Exception placing order for {scrip_code}: {e}. Retry {i+1}/3.")
            time.sleep(5)
    return None

def place_strangle_order():
    global trade_is_active, ce_scrip_code, pe_scrip_code, entry_data, active_legs, pending_sl_order_ids
    if trade_is_active: return
    logging.info("Attempting to place strangle order...")

    nearest_expiry = get_nearest_weekly_expiry(config.SYMBOL)
    if not nearest_expiry: return

    option_chain = get_option_chain(config.SYMBOL, nearest_expiry)
    if not option_chain: return

    spot_price = get_spot_price(config.SYMBOL)
    if not spot_price: return

    ce_strike, pe_strike = select_strikes(option_chain, spot_price)
    if not ce_strike or not pe_strike:
        logging.error("Could not select strikes.")
        return

    ce_scrip_code = next((o['ScripCode'] for o in option_chain if o['StrikeRate'] == ce_strike and o['CPType'] == 'CE'), None)
    pe_scrip_code = next((o['ScripCode'] for o in option_chain if o['StrikeRate'] == pe_strike and o['CPType'] == 'PE'), None)

    if not ce_scrip_code or not pe_scrip_code:
        logging.error("Could not find scrip codes for selected strikes.")
        return

    if config.PAPER_TRADING:
        trade_is_active = True
        active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
        entry_data = {ce_scrip_code: {'strike': ce_strike, 'entry_price': 100}, pe_scrip_code: {'strike': pe_strike, 'entry_price': 100}}
        logging.info("Paper trade is now active.")
        return

    results = {}
    order_lock = threading.Lock()
    def place_leg(leg_type, scrip):
        results[leg_type] = _place_order_with_retry('S', scrip, config.QTY, lock=order_lock)

    ce_thread = threading.Thread(target=place_leg, args=('CE', ce_scrip_code))
    pe_thread = threading.Thread(target=place_leg, args=('PE', pe_scrip_code))
    ce_thread.start()
    pe_thread.start()
    ce_thread.join()
    pe_thread.join()

    if results.get('CE') and results.get('PE'):
        for i in range(12):
            positions = client.positions()
            ce_pos = next((p for p in (positions or []) if p['ScripCode'] == ce_scrip_code), None)
            pe_pos = next((p for p in (positions or []) if p['ScripCode'] == pe_scrip_code), None)
            if ce_pos and pe_pos:
                entry_data = {ce_scrip_code: {'strike': ce_strike, 'entry_price': ce_pos['SellAvgRate']}, pe_scrip_code: {'strike': pe_strike, 'entry_price': pe_pos['SellAvgRate']}}
                sl_ce_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=ce_scrip_code, Qty=abs(ce_pos['NetQty']), Price=ce_pos['SellAvgRate'] + config.LEG_WISE_SL_POINTS + config.SL_LIMIT_BUFFER, StopLossPrice=ce_pos['SellAvgRate'] + config.LEG_WISE_SL_POINTS, IsIntraday=True)
                if sl_ce_order.get('Status') == 0: pending_sl_order_ids.append(sl_ce_order.get('BrokerOrderID'))
                sl_pe_order = client.place_order(OrderType='B', Exchange='N', ExchangeType='D', ScripCode=pe_scrip_code, Qty=abs(pe_pos['NetQty']), Price=pe_pos['SellAvgRate'] + config.LEG_WISE_SL_POINTS + config.SL_LIMIT_BUFFER, StopLossPrice=pe_pos['SellAvgRate'] + config.LEG_WISE_SL_POINTS, IsIntraday=True)
                if sl_pe_order.get('Status') == 0: pending_sl_order_ids.append(sl_pe_order.get('BrokerOrderID'))
                ws_manager.subscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in [ce_scrip_code, pe_scrip_code]])
                trade_is_active = True
                active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
                logging.info(f"Trade is now active. CE@{entry_data[ce_scrip_code]['entry_price']:.2f}, PE@{entry_data[pe_scrip_code]['entry_price']:.2f}")
                return
            time.sleep(5)
        logging.critical("Failed to confirm position execution. Exiting.")
    else:
        logging.critical("One or both legs failed to place. Squaring off if necessary.")
        square_off_lock = threading.Lock()
        if results.get('CE'): _place_order_with_retry('B', ce_scrip_code, config.QTY, lock=square_off_lock)
        if results.get('PE'): _place_order_with_retry('B', pe_scrip_code, config.QTY, lock=square_off_lock)

def get_current_pnl():
    if not trade_is_active: return 0
    pnl = realized_pnl
    for leg, scrip in active_legs.items():
        if scrip in ltp_store and scrip in entry_data:
            pnl += (entry_data[scrip]['entry_price'] - ltp_store[scrip]) * config.QTY
    return pnl

def check_trade_conditions():
    global max_pnl, trailing_sl_activated, current_trailing_sl
    pnl = get_current_pnl()
    if pnl <= config.OVERALL_SL or pnl >= config.OVERALL_TARGET:
        reason = 'OVERALL_SL_HIT' if pnl <= config.OVERALL_SL else 'OVERALL_TARGET_HIT'
        action_queue.put({'action': 'exit', 'reason': reason})
    elif trailing_sl_activated:
        if pnl > max_pnl: max_pnl = pnl
        tsl_level = (int(max_pnl / config.TRAILING_PROFIT_TRIGGER)) * config.TRAILING_PROFIT_LOCKIN
        current_trailing_sl = tsl_level
        if pnl < tsl_level: action_queue.put({'action': 'exit', 'reason': 'TRAILING_SL_HIT'})
    elif pnl >= config.TRAILING_PROFIT_TRIGGER:
        trailing_sl_activated = True
        max_pnl = pnl
        logging.info(f"Trailing SL activated at P&L: {pnl:,.2f}")

def exit_positions(reason="Unknown"):
    global trade_is_active, pending_sl_order_ids, entry_data, active_legs, realized_pnl, max_pnl, trailing_sl_activated, ce_scrip_code, pe_scrip_code, current_trailing_sl
    if not trade_is_active: return
    logging.info(f"Exiting positions due to: {reason}")
    if ws_manager and active_legs:
        ws_manager.unsubscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in active_legs.values()])

    if not config.PAPER_TRADING:
        for order_id in pending_sl_order_ids:
            try: client.cancel_order(order_id)
            except: pass
        exit_lock = threading.Lock()
        for leg, scrip in list(active_legs.items()):
            _place_order_with_retry('B', scrip, config.QTY, lock=exit_lock)

    log_trade_to_csv({
        'Date': datetime.date.today().isoformat(), 'Symbol': config.SYMBOL, 'EntryTime': config.ENTRY_TIME,
        'ExitTime': datetime.datetime.now().strftime("%H:%M:%S"), 'Call_Strike': entry_data.get(ce_scrip_code, {}).get('strike', 0),
        'Put_Strike': entry_data.get(pe_scrip_code, {}).get('strike', 0), 'Call_Entry_Premium': entry_data.get(ce_scrip_code, {}).get('entry_price', 0),
        'Put_Entry_Premium': entry_data.get(pe_scrip_code, {}).get('entry_price', 0),
        'Call_Exit_Price': ltp_store.get(ce_scrip_code, 0), 'Put_Exit_Price': ltp_store.get(pe_scrip_code, 0),
        'Final_PnL': get_current_pnl(), 'Exit_Reason': reason, 'Trade_Mode': 'PAPER' if config.PAPER_TRADING else 'LIVE'
    })

    trade_is_active, trailing_sl_activated = False, False
    pending_sl_order_ids, entry_data, active_legs, realized_pnl, max_pnl, current_trailing_sl, ce_scrip_code, pe_scrip_code = [], {}, {}, 0, 0, 0, None, None

def monitor_positions():
    global realized_pnl
    if not trade_is_active: return
    live_scrip_codes = {p['ScripCode'] for p in (client.positions() or [])}
    for leg, scrip in list(active_legs.items()):
        if scrip not in live_scrip_codes:
            logging.warning(f"{leg} leg closed (likely SL hit).")
            pnl = (entry_data[scrip]['entry_price'] - ltp_store.get(scrip, entry_data[scrip]['entry_price'])) * config.QTY
            realized_pnl += pnl
            del active_legs[leg]
            if config.EXIT_STRATEGY_ON_LEG_SL_HIT:
                action_queue.put({'action': 'exit', 'reason': 'LEG_SL_HIT_EXIT'})
                return
    if not active_legs:
        action_queue.put({'action': 'exit', 'reason': 'BOTH_LEGS_CLOSED'})

def log_pnl_status():
    if not trade_is_active: return
    pnl = get_current_pnl()
    if trailing_sl_activated:
        logging.info(f"P&L: {pnl:,.2f} | Max P&L: {max_pnl:,.2f} | Trailing SL: {current_trailing_sl:,.2f}")
    else:
        logging.info(f"P&L: {pnl:,.2f}")

def log_trade_to_csv(data):
    file_path = os.path.join(os.path.dirname(__file__), 'trade_log.csv')
    with open(file_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not isfile(file_path) or os.path.getsize(file_path) == 0: writer.writeheader()
        writer.writerow(data)

def handle_user_commands():
    """Handles manual user commands from the terminal."""
    global config
    logging.info("CLI enabled. Type 'SL <value>', 'TP <value>', 'TRAIL <value>', 'LOCK <value>', or 'EXIT'.")
    while True:
        try:
            command = input("").strip().upper()
            if not command: continue
            parts = command.split()
            cmd = parts[0]
            if cmd == 'EXIT':
                action_queue.put({'action': 'exit', 'reason': 'MANUAL_EXIT'})
                continue
            if len(parts) < 2: continue
            value = float(parts[1])
            if cmd == 'SL': config.OVERALL_SL = -abs(value)
            elif cmd == 'TP': config.OVERALL_TARGET = value
            elif cmd == 'TRAIL': config.TRAILING_PROFIT_TRIGGER = value
            elif cmd == 'LOCK': config.TRAILING_PROFIT_LOCKIN = value
            logging.info(f"Updated {cmd} to {value}")
        except (EOFError, KeyboardInterrupt): break
        except Exception as e: logging.error(f"CLI Error: {e}")

if __name__ == "__main__":
    ws_manager = WebSocketManager()
    ws_manager.start()
    time.sleep(5)

    cli_thread = threading.Thread(target=handle_user_commands, daemon=True)
    cli_thread.start()

    if len(sys.argv) > 1 and sys.argv[1] == '--now':
        place_strangle_order()
    else:
        schedule.every().day.at(config.ENTRY_TIME).do(place_strangle_order)
        schedule.every().day.at(config.EXIT_TIME).do(lambda: action_queue.put({'action': 'exit', 'reason': 'TIMED_EXIT'}))

    schedule.every(5).seconds.do(monitor_positions)
    schedule.every(10).seconds.do(log_pnl_status)

    while True:
        schedule.run_pending()
        try:
            action = action_queue.get_nowait()
            if action.get('action') == 'exit':
                exit_positions(reason=action.get('reason'))
                if len(sys.argv) > 1 and sys.argv[1] == '--now': break # Exit script if in --now mode
        except queue.Empty:
            pass
        time.sleep(1)