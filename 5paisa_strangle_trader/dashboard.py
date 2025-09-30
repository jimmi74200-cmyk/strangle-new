import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash
import ast

app = Flask(__name__)
app.secret_key = 'super_secret_key_for_flash_messages'

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.py')

# Define the parameters and their expected types for validation
CONFIG_SCHEMA = {
    "Trading Parameters": {
        "PAPER_TRADING": bool,
        "SYMBOL": str,
        "QTY": int,
        "ENTRY_TIME": "time",
        "EXIT_TIME": "time"
    },
    "Strike Selection Parameters": {
        "STRIKE_SELECTION_METHOD": ["NEAREST_PREMIUM", "EQUAL_PREMIUM_GAP", "OTM", "ITM", "ATM"],
        "PREMIUM": float,
        "STRANGLE_STRIKE_DISTANCE": int,
        "STRANGLE_GAP_POINTS": float
    },
    "Risk Management Parameters": {
        "LEG_WISE_SL_POINTS": float,
        "SL_LIMIT_BUFFER": float,
        "OVERALL_SL": float,
        "OVERALL_TARGET": float,
        "TRAILING_PROFIT_TRIGGER": float,
        "TRAILING_PROFIT_LOCKIN": float,
        "EXIT_STRATEGY_ON_LEG_SL_HIT": bool
    }
}

def validate_form_data(form_data):
    """
    Validates the submitted form data against the CONFIG_SCHEMA.
    Returns True if valid, False otherwise. Flashes error messages.
    """
    is_valid = True
    for group, params in CONFIG_SCHEMA.items():
        for key, expected_type in params.items():
            if key not in form_data:
                continue # Should not happen with a proper form

            value = form_data[key]

            try:
                if expected_type == bool:
                    if value not in ['True', 'False']:
                        raise ValueError("Invalid boolean")
                elif expected_type == int:
                    int(value)
                elif expected_type == float:
                    float(value)
                elif expected_type == "time":
                    if not re.match(r'^\d{2}:\d{2}$', value):
                        raise ValueError("Invalid time format")
                elif isinstance(expected_type, list): # For dropdowns
                    if value not in expected_type:
                        raise ValueError("Invalid option selected")
            except ValueError:
                is_valid = False
                flash(f"Invalid value for '{key}'. Please provide a valid input (e.g., a number, True/False, or HH:MM format).", "danger")
                # Stop after first error to avoid spamming messages
                return False
    return is_valid

def get_config_values():
    config_values = {}
    all_keys = [key for group in CONFIG_SCHEMA.values() for key in group.keys()]
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            content = f.read()
            for key in all_keys:
                pattern = re.compile(rf"^{key}\s*=\s*(.*)", re.MULTILINE)
                match = pattern.search(content)
                if match:
                    # Use ast.literal_eval to safely parse Python literals (strings, numbers, booleans)
                    try:
                        val = ast.literal_eval(match.group(1).strip())
                        config_values[key] = val
                    except (ValueError, SyntaxError):
                         # Fallback for simple string stripping if literal_eval fails
                        config_values[key] = match.group(1).strip().strip("'\"")

    except Exception as e:
        flash(f"Error reading config file: {e}", "danger")

    categorized_config = {group: {key: config_values.get(key, '') for key in keys} for group, keys in CONFIG_SCHEMA.items()}
    return categorized_config


def save_config_values(new_values):
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            lines = f.readlines()

        with open(CONFIG_FILE_PATH, 'w') as f:
            for line in lines:
                match = re.match(r"^(\w+)\s*=", line)
                if match:
                    key = match.group(1)
                    if key in new_values:
                        # repr() is a great way to get the Python representation of an object
                        # e.g., 'string' -> "'string'", 123 -> '123', True -> 'True'
                        f.write(f'{key} = {repr(new_values[key])}\n')
                    else:
                        f.write(line)
                else:
                    f.write(line)
        flash("Configuration saved successfully!", "success")
    except Exception as e:
        flash(f"Error saving config file: {e}", "danger")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        form_data = request.form.to_dict()
        if validate_form_data(form_data):
            # Convert form strings to correct Python types before saving
            typed_data = {}
            for key, value_str in form_data.items():
                for params in CONFIG_SCHEMA.values():
                    if key in params:
                        expected_type = params[key]
                        try:
                            if expected_type == bool:
                                typed_data[key] = (value_str == 'True')
                            elif expected_type == int:
                                typed_data[key] = int(value_str)
                            elif expected_type == float:
                                typed_data[key] = float(value_str)
                            else: # Handles str, time, and list validation cases
                                typed_data[key] = value_str
                        except ValueError:
                            # This should already be caught by validate_form_data, but as a safeguard:
                            flash(f"Data type conversion failed for {key}. Aborting save.", "danger")
                            return redirect(url_for('index'))
            save_config_values(typed_data)
        return redirect(url_for('index'))

    config_data = get_config_values()
    # Pass the isinstance function to the template context so it can be used for type checking
    return render_template('index.html', categorized_config=config_data, schema=CONFIG_SCHEMA, isinstance=isinstance)

if __name__ == '__main__':
    app.run(debug=True, port=5001)