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
current_trailing_sl = 0
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

def get_current_pnl():
    if not trade_is_active: return 0
    pnl = 0
    if 'CE' in active_legs and ce_scrip_code in ltp_store:
        pnl += (entry_data[ce_scrip_code]['entry_price'] - ltp_store[ce_scrip_code]) * config.QTY
    if 'PE' in active_legs and pe_scrip_code in ltp_store:
        pnl += (entry_data[pe_scrip_code]['entry_price'] - ltp_store[pe_scrip_code]) * config.QTY
    return pnl

def check_trade_conditions():
    global max_pnl, trailing_sl_activated, current_trailing_sl
    if not trade_is_active: return
    pnl = get_current_pnl()

    if pnl <= config.OVERALL_SL: action_queue.put({'action': 'exit', 'reason': 'OVERALL_SL_HIT'})
    elif pnl >= config.OVERALL_TARGET: action_queue.put({'action': 'exit', 'reason': 'OVERALL_TARGET_HIT'})
    elif trailing_sl_activated:
        if pnl > max_pnl: max_pnl = pnl
        # New "fixed drawdown" trailing stop-loss logic
        tsl_level = max_pnl - config.TRAILING_PROFIT_LOCKIN
        current_trailing_sl = tsl_level
        if pnl < tsl_level: action_queue.put({'action': 'exit', 'reason': 'TRAILING_SL_HIT'})
    elif pnl >= config.TRAILING_PROFIT_TRIGGER:
        trailing_sl_activated = True
        max_pnl = pnl
        logging.info(f"Trailing SL activated at P&L: {pnl:,.2f}")

def exit_positions(reason="Unknown"):
    global trade_is_active
    if not trade_is_active: return
    logging.info(f"EXIT TRIGGERED: Exiting all positions due to: {reason}")
    if ws_manager and active_legs:
        ws_manager.unsubscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in active_legs.values()])

    if not config.PAPER_TRADING:
        logging.info("Searching for and cancelling pending SL orders...")
        try:
            order_book = client.order_book()
            if order_book:
                sl_orders = [o for o in order_book if o.get('ScripCode') in [ce_scrip_code, pe_scrip_code] and o.get('OrderStatus', '').strip() in ['Pending', 'Modified', 'Trigger Pending', 'Open'] and float(o.get('SLTriggerRate', 0)) > 0]
                for order in sl_orders:
                    client.cancel_order(order.get('BrokerOrderId'))
        except Exception as e:
            logging.error(f"Error cancelling SL orders: {e}")

        exit_lock = threading.Lock()
        for leg, scrip in list(active_legs.items()):
            _place_order_with_retry('B', scrip, config.QTY, lock=exit_lock)

    trade_is_active = False
    logging.info("All exit orders placed. Position is now closed.")

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

def adopt_open_positions():
    global trade_is_active, ce_scrip_code, pe_scrip_code, entry_data, active_legs
    logging.info("Scanning for existing open strangle positions...")
    try:
        positions = client.positions()
        if not positions: return False
        symbol_positions = [p for p in positions if config.SYMBOL in p.get('ScripName', '') and p.get('NetQty', 0) < 0]
        ce_pos = next((p for p in symbol_positions if " CE " in p.get('ScripName', '')), None)
        pe_pos = next((p for p in symbol_positions if " PE " in p.get('ScripName', '')), None)
        if ce_pos and pe_pos:
            ce_scrip_code = ce_pos['ScripCode']
            pe_scrip_code = pe_pos['ScripCode']
            def get_strike(name): return float(re.search(r'(\d+(\.\d+)?)$', name.strip()).group(1))
            entry_data = {ce_scrip_code: {'strike': get_strike(ce_pos['ScripName']), 'entry_price': ce_pos['SellAvgRate']}, pe_scrip_code: {'strike': get_strike(pe_pos['ScripName']), 'entry_price': pe_pos['SellAvgRate']}}
            active_legs = {'CE': ce_scrip_code, 'PE': pe_scrip_code}
            trade_is_active = True
            logging.info(f"Adopted CE: {ce_pos['ScripName']} @ {ce_pos['SellAvgRate']:.2f}")
            logging.info(f"Adopted PE: {pe_pos['ScripName']} @ {pe_pos['SellAvgRate']:.2f}")
            ws_manager.subscribe([{"Exch": "N", "ExchType": "D", "ScripCode": sc} for sc in [ce_scrip_code, pe_scrip_code]])
            return True
        return False
    except Exception as e:
        logging.error(f"Error while scanning for positions: {e}")
        return False

def log_pnl_status():
    if not trade_is_active: return
    pnl = get_current_pnl()
    if trailing_sl_activated:
        logging.info(f"P&L: {pnl:,.2f} | Max P&L: {max_pnl:,.2f} | Trailing SL: {current_trailing_sl:,.2f}")
    else:
        logging.info(f"P&L: {pnl:,.2f}")

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
    client = FivePaisaClient(cred={"APP_NAME": config.APP_NAME, "APP_SOURCE": config.APP_SOURCE, "USER_ID": config.USER_ID, "PASSWORD": config.PASSWORD, "USER_KEY": config.USER_KEY, "ENCRYPTION_KEY": config.ENCRYPTION_KEY})
    client.set_access_token(config.ACCESS_TOKEN, config.CLIENT_CODE)
    ws_manager = WebSocketManager()
    ws_manager.start()
    time.sleep(5)

    if not ws_manager.is_connected:
        logging.critical("WebSocket connection failed.")
        sys.exit(1)

    if adopt_open_positions():
        logging.info("Position adopted. Manual override commands are now active.")
        cli_thread = threading.Thread(target=handle_user_commands, daemon=True)
        cli_thread.start()

        while trade_is_active:
            log_pnl_status()
            try:
                action = action_queue.get_nowait()
                if action.get('action') == 'exit':
                    exit_positions(reason=action.get('reason'))
            except queue.Empty:
                pass
            time.sleep(10)
    else:
        logging.info("No active strangle position to manage. Exiting.")

    if ws_manager: ws_manager.is_connected = False
    sys.exit(0)