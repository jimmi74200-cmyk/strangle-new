# 5paisa Automated Intraday Option Strangle Strategy

This project is a Python-based system for automating an intraday option strangle selling strategy using the 5paisa API. It includes a main trading bot, a standalone exit manager, and a web-based dashboard for configuration.

## Features

-   **Automated Trading:** Automatically places and exits short strangle trades based on a schedule.
-   **Flexible Strike Selection:** Multiple methods for strike selection (ATM, OTM, Nearest Premium, etc.).
-   **Advanced Risk Management:** Manages overall P&L with a target, stop-loss, and a dynamic trailing stop-loss.
-   **Web Dashboard:** A user-friendly web interface to view and edit the strategy configuration (`config.py`) safely.
-   **Standalone Exit Manager:** A separate script to take over management of an existing trade, providing robustness in case the main bot is stopped.
-   **Live Manual Overrides:** A command-line interface (CLI) to adjust risk parameters (SL, TP, Trail) or exit a trade in real-time.
-   **Paper Trading Mode:** Test your strategy without risking real capital.
-   **Detailed Logging:** Logs all trades to a `trade_log.csv` file for performance analysis.
-   **Secure Authentication:** Supports both TOTP and browser-based login flows to generate daily access tokens.

## Project Components

This repository contains several key scripts that work together:

-   `trader.py`: The main, fully automated trading bot. It handles the entire lifecycle of a trade, from entry to management to exit.
-   `exit_manager.py`: A standalone script designed to manage a position that is already open. If `trader.py` stops for any reason, you can run this script to "adopt" the open trade and manage it with the configured risk rules.
-   `dashboard.py`: A web-based configuration dashboard. It provides a simple GUI to view and edit the parameters in `config.py`, reducing the risk of manual errors.
-   `authenticate.py`: A script for fast, non-browser authentication using your TOTP (from an authenticator app). Recommended for servers.
-   `authenticate_browser.py`: A script that uses your web browser to log in and fetch the daily access token.
-   `config.py`: The central configuration file for all scripts.

## How to Use

### 1. Installation

1.  Clone this repository.
2.  Install the required Python libraries from the `requirements.txt` file:
    ```bash
    pip install -r 5paisa_strangle_trader/requirements.txt
    ```

### 2. Configuration

You can configure the bot in two ways:

#### Method A: Using the Web Dashboard (Recommended)

1.  Run the dashboard script:
    ```bash
    python 5paisa_strangle_trader/dashboard.py
    ```
2.  Open your web browser and go to `http://127.0.0.1:5001`.
3.  Modify your trading, strike selection, and risk parameters in the user-friendly interface and click "Save".

#### Method B: Manually Editing `config.py`

1.  Open `5paisa_strangle_trader/config.py` in a text editor.
2.  Fill in your 5paisa API credentials (`APP_NAME`, `USER_KEY`, etc.) one time.
3.  Adjust any trading parameters as needed.

### 3. Daily Authentication (Choose One Method)

Before running the bot each day, you must generate a new access token.

-   **TOTP-based:** Run `python 5paisa_strangle_trader/authenticate.py` and follow the prompts.
-   **Browser-based:** Run `python 5paisa_strangle_trader/authenticate_browser.py`.

Both scripts will automatically update the `ACCESS_TOKEN` in your `config.py` file.

### 4. Running the Main Bot

To start the fully automated trading bot, run `trader.py`:
```bash
python 5paisa_strangle_trader/trader.py
```
The bot will now wait for the `ENTRY_TIME` to place a trade and will manage it until `EXIT_TIME` or until a risk condition is met.

### 5. Running the Exit Manager (If Needed)

If `trader.py` has stopped but you have an open position, run `exit_manager.py` to take over:
```bash
python 5paisa_strangle_trader/exit_manager.py
```
It will scan your account, adopt the position, and start managing it immediately.

## Manual CLI Commands

While `trader.py` or `exit_manager.py` is running, you can take manual control by typing commands into the same terminal window.

| Command      | Example      | Description                                                 |
| :----------- | :----------- | :---------------------------------------------------------- |
| `SL`         | `SL 800`     | Sets the **Overall Stop-Loss** to **-800**. (Value is made negative automatically). |
| `TP`         | `TP 6000`    | Sets the **Overall Target** to **6000**.                      |
| `TRAIL`      | `TRAIL 3000` | Sets the **Trailing Profit Trigger** to **3000**.             |
| `LOCK`       | `LOCK 500`   | Sets the **Trailing Profit Lock-in** value to **500**.        |
| `EXIT`       | `EXIT`       | Immediately triggers a market exit for the current position. |

The script will log a confirmation message for each successful command.

## Disclaimer

This script is for educational purposes only. Use it at your own risk. The author is not responsible for any financial losses.