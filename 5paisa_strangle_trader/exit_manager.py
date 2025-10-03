import datetime
import logging
import time
from py5paisa import FivePaisaClient
import config
import json
import threading
import sys
import websockets
import asyncio
import queue
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global State ---
client = None
ltp_store = {}
action_queue = queue.Queue()
entry_data = {}
max_pnl = 0
trailing_sl_activated = False
current_trailing_sl = 0 # New global to track the TSL level
ce_scrip_code = None
pe_scrip_code = None
realized_pnl = 0
trade_is_active = False
ws_manager = None
active_legs = {}

class WebSocketManager:
    def __init__(self):
        self._ws_url = f"wss://openfeed.5paisa.com/feeds/api/chat?Value1={config.ACCESS_TOKEN}|{config.CLIENT_CODE}"
        self._thread = None
        self._subscription_queue = queue.Queue()
        self.is_connected = False

    async def _run(self):
        logging.info("Attempting to connect to websocket...")
        try:
            async with websockets.connect(self._ws_url) as websocket:
                self.is_connected = True
                logging.info("WebSocket connected successfully.")
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

def get_current_pnl():
    """Calculates the current P&L for all active legs."""
    if not trade_is_active: return 0
    current_pnl = 0
    if 'CE' in active_legs and ce_scrip_code in ltp_store:
        current_pnl += (entry_data[ce_scrip_code]['entry_price'] - ltp_store[ce_scrip_code]) * config.QTY
    if 'PE' in active_legs and pe_scrip_code in ltp_store:
        current_pnl += (entry_data[pe_scrip_code]['entry_price'] - ltp_store[pe_scrip_code]) * config.QTY
    return current_pnl

def check_trade_conditions():
    global max_pnl, trailing_sl_activated, current_trailing_sl
    if not trade_is_active: return
    total_pnl = get_current_pnl()

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
    global trade_is_active
    if not trade_is_active: return
    logging.info(f"EXIT TRIGGERED: Exiting all positions due to: {reason}")

    if ws_manager and active_legs:
        ws_manager.unsubscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in active_legs.values()])

    if not config.PAPER_TRADING:
        logging.info("Searching for and cancelling pending SL orders for the position...")
        try:
            order_book = client.order_book()
            if order_book:
                sl_orders_to_cancel = [o for o in order_book if o.get('ScripCode') in [ce_scrip_code, pe_scrip_code] and o.get('OrderStatus', '').strip() in ['Pending', 'Modified', 'Trigger Pending', 'Open'] and float(o.get('SLTriggerRate', 0)) > 0]
                for order in sl_orders_to_cancel:
                    order_id = order.get('BrokerOrderId')
                    logging.info(f"Found pending SL order {order_id}. Cancelling...")
                    client.cancel_order(order_id)
        except Exception as e:
            logging.error(f"An error occurred while trying to cancel SL orders: {e}")

        exit_lock = threading.Lock()
        for leg_type, scrip_code in list(active_legs.items()):
            logging.info(f"Placing market order to square off {leg_type} leg (ScripCode: {scrip_code}).")
            _place_order_with_retry('B', scrip_code, config.QTY, lock=exit_lock, max_retries=5)

    logging.info("All exit orders placed. Position is now closed.")
    trade_is_active = False

def _place_order_with_retry(order_type, scrip_code, qty, lock, max_retries=3, retry_delay=5, **kwargs):
    for i in range(max_retries):
        try:
            with lock:
                order_result = client.place_order(OrderType=order_type, Exchange='N', ExchangeType='D', ScripCode=scrip_code, Qty=qty, Price=0, IsIntraday=True, **kwargs)
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

def adopt_open_positions():
    global trade_is_active, ce_scrip_code, pe_scrip_code, active_legs, entry_data
    logging.info("Scanning for existing open strangle positions...")
    try:
        positions = client.positions()
        if not positions:
            logging.info("No open positions found.")
            return False

        symbol_positions = [p for p in positions if config.SYMBOL in p.get('ScripName', '') and p.get('NetQty', 0) < 0]
        ce_pos = next((p for p in symbol_positions if " CE " in p.get('ScripName', '')), None)
        pe_pos = next((p for p in symbol_positions if " PE " in p.get('ScripName', '')), None)

        if ce_pos and pe_pos:
            logging.info("Found an existing strangle position. Adopting it now.")
            def get_strike_from_name(scrip_name):
                match = re.search(r'(\d+(\.\d+)?)$', scrip_name.strip())
                if match: return float(match.group(1))
                return None

            ce_strike = get_strike_from_name(ce_pos['ScripName'])
            pe_strike = get_strike_from_name(pe_pos['ScripName'])
            if not ce_strike or not pe_strike:
                logging.error("Could not parse strike price from ScripName for one or both legs.")
                return False

            ce_scrip_code = ce_pos['ScripCode']
            pe_scrip_code = pe_pos['ScripCode']
            entry_data[ce_scrip_code] = {'strike': ce_strike, 'entry_price': ce_pos['SellAvgRate']}
            entry_data[pe_scrip_code] = {'strike': pe_strike, 'entry_price': pe_pos['SellAvgRate']}
            active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
            trade_is_active = True

            logging.info(f"Adopted CE leg: {ce_pos['ScripName']} @ {ce_pos['SellAvgRate']}")
            logging.info(f"Adopted PE leg: {pe_pos['ScripName']} @ {pe_pos['SellAvgRate']}")
            ws_manager.subscribe([{"Exch": "N", "ExchType": "D", "ScripCode": ce_scrip_code}, {"Exch": "N", "ExchType": "D", "ScripCode": pe_scrip_code}])
            logging.info("Subscribed to market data for the adopted position.")
            return True
        else:
            logging.info("Could not find a valid strangle position (one short CE and one short PE).")
            return False
    except Exception as e:
        logging.error(f"An error occurred while scanning for positions: {e}")
        return False

def log_pnl_status():
    """Calculates and logs the current P&L and TSL status."""
    if not trade_is_active: return
    current_pnl = get_current_pnl()
    if trailing_sl_activated:
        logging.info(f"P&L: {current_pnl:,.2f} | Max P&L: {max_pnl:,.2f} | Trailing SL: {current_trailing_sl:,.2f}")
    else:
        logging.info(f"P&L: {current_pnl:,.2f}")

if __name__ == "__main__":
    logging.info("--- Starting Exit Manager Script ---")

    client = FivePaisaClient(cred={"APP_NAME": config.APP_NAME, "APP_SOURCE": config.APP_SOURCE, "USER_ID": config.USER_ID, "PASSWORD": config.PASSWORD, "USER_KEY": config.USER_KEY, "ENCRYPTION_KEY": config.ENCRYPTION_KEY})
    client.set_access_token(config.ACCESS_TOKEN, config.CLIENT_CODE)

    ws_manager = WebSocketManager()
    ws_manager.start()
    logging.info("Waiting for websocket to connect...")
    time.sleep(5)

    if not ws_manager.is_connected:
        logging.critical("WebSocket connection failed. Cannot proceed.")
        sys.exit(1)

    if adopt_open_positions():
        logging.info("Position adopted successfully. Entering monitoring mode.")
        last_pnl_log_time = time.time()
        while trade_is_active:
            if time.time() - last_pnl_log_time > 10:
                log_pnl_status()
                last_pnl_log_time = time.time()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit':
                    exit_positions(reason=action.get('reason'))
            except queue.Empty:
                pass
            time.sleep(1)
        logging.info("Monitoring has ended because the trade was closed.")
    else:
        logging.info("No active strangle position to manage. Exiting script.")

    logging.info("--- Exit Manager Script Finished ---")
    if ws_manager:
        ws_manager.is_connected = False
    sys.exit(0)