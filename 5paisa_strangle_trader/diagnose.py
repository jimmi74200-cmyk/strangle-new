import logging
from py5paisa import FivePaisaClient
import config
import json

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def diagnose_order_book():
    """
    A utility script to fetch and print all orders from the order book for diagnostics.
    """
    print("--- NEW DIAGNOSTIC SCRIPT (diagnose.py) ---")
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

    logging.info("Fetching ALL orders from the order book for diagnostics...")
    try:
        order_book = client.order_book()
        if not order_book:
            logging.info("Order book is completely empty. No orders found.")
            return

        logging.info(f"Found {len(order_book)} total order(s) in the order book. Printing all details below:")
        print("=" * 70)

        # Loop through and print every order with all its details
        for i, order in enumerate(order_book):
            print(f"--- DETAILS FOR ORDER #{i+1} ---")
            # Pretty print the dictionary for clear viewing
            print(json.dumps(order, indent=2))
            print("=" * 70)

        logging.info("--- DIAGNOSTIC PRINT COMPLETE ---")
        logging.info("Please find your pending stop-loss order in the output above.")
        logging.info("I need to know the exact string value of the 'OrderStatus' field for that order.")

    except Exception as e:
        logging.error(f"An error occurred while fetching the order book: {e}")

if __name__ == "__main__":
    diagnose_order_book()