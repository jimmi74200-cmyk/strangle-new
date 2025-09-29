import logging
from py5paisa import FivePaisaClient
import config

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_sl_cancellation():
    """
    A utility script to find pending stop-loss orders and cancel one specified by the user.
    """
    logging.info("Connecting to 5paisa client...")
    try:
        client = FivePaisaClient(cred={
            "APP_NAME": config.APP_NAME,
            "APP_SOURCE": config.APP_SOURCE,
            "USER_ID": config.USER_ID,
            "PASSWORD": config.PASSWORD,
            "USER_KEY": config.USER_KEY,
            "ENCRYPTION_KEY": config.ENCRYPTION_KEY
        })
        client.set_access_token(config.ACCESS_TOKEN, config.CLIENT_CODE)
        logging.info("Successfully connected.")
    except Exception as e:
        logging.error(f"Failed to connect to 5paisa client: {e}")
        return

    logging.info("Fetching order book to find pending stop-loss orders...")
    try:
        order_book = client.order_book()
        if not order_book:
            logging.info("Order book is empty. No orders found.")
            return

        # Filter for orders that are pending and are stop-loss orders.
        # Common statuses for pending SL orders are 'Pending' or 'Trigger Pending'.
        # A stop-loss order is identified by having a StopLossPrice > 0.
        pending_sl_orders = [
            order for order in order_book
            if order.get('OrderStatus') in ['Pending', 'Trigger Pending'] and order.get('StopLossPrice', 0) > 0
        ]

        if not pending_sl_orders:
            logging.info("No pending stop-loss orders found.")
            return

        logging.info(f"Found {len(pending_sl_orders)} pending stop-loss order(s):")
        print("-" * 50)
        for order in pending_sl_orders:
            print(f"  Broker Order ID: {order.get('BrokerOrderId')}")
            print(f"  Scrip Name:      {order.get('ScripName')}")
            print(f"  Quantity:        {order.get('Qty')}")
            print(f"  Stop-Loss Price: {order.get('StopLossPrice')}")
            print("-" * 50)

        # Prompt user for the ID to cancel
        try:
            target_order_id = input("Please enter the Broker Order ID of the order you want to cancel: ").strip()
            if not target_order_id:
                print("No Order ID entered. Exiting.")
                return

            # Find the chosen order to confirm details before cancelling
            order_to_cancel = next((o for o in pending_sl_orders if str(o.get('BrokerOrderId')) == target_order_id), None)

            if not order_to_cancel:
                print(f"Error: Order ID {target_order_id} not found in the list of pending SL orders.")
                return

            print(f"\nAttempting to cancel order {target_order_id} ({order_to_cancel.get('ScripName')})...")

            # Perform the cancellation
            cancellation_result = client.cancel_order(target_order_id)

            # The API returns a list with a dictionary inside on success
            if cancellation_result and isinstance(cancellation_result, list) and cancellation_result[0].get('Status') == 0:
                logging.info(f"Successfully sent cancellation request for order {target_order_id}.")
                print(f"Message from server: {cancellation_result[0].get('Message')}")
            else:
                logging.error(f"Failed to cancel order {target_order_id}.")
                print(f"API Response: {cancellation_result}")

        except (KeyboardInterrupt, EOFError):
            print("\nOperation cancelled by user. Exiting.")
        except Exception as e:
            logging.error(f"An error occurred during cancellation: {e}")

    except Exception as e:
        logging.error(f"Failed to fetch order book or process orders: {e}")

if __name__ == "__main__":
    test_sl_cancellation()