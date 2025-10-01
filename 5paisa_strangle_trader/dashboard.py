import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash
import ast

app = Flask(__name__)
app.secret_key = 'super_secret_key_for_flash_messages'

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.py')

# Define the parameters, their types, and options for validation and rendering
CONFIG_SCHEMA = {
    "Trading Parameters": {
        "PAPER_TRADING": bool,
        "SYMBOL": ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"], # Provide a list for dropdown
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
    is_valid = True
    for group, params in CONFIG_SCHEMA.items():
        for key, expected_type in params.items():
            if key not in form_data:
                continue

            value = form_data[key]

            try:
                if expected_type == bool:
                    if value not in ['True', 'False']:
                        raise ValueError("Invalid boolean")
                elif expected_type == int:
                    int(value)
                elif expected_type == float:
                    num_value = float(value)
                    # Add specific validation for OVERALL_SL
                    if key == 'OVERALL_SL' and num_value > 0:
                        flash("Invalid value for 'OVERALL_SL'. It must be a negative number or zero.", "danger")
                        return False
                elif expected_type == "time":
                    if not re.match(r'^\d{2}:\d{2}$', value):
                        raise ValueError("Invalid time format")
                elif isinstance(expected_type, list):
                    if value not in expected_type:
                        raise ValueError("Invalid option selected")
            except ValueError:
                is_valid = False
                flash(f"Invalid value for '{key}'. Please provide a valid input.", "danger")
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
                    try:
                        val_str = match.group(1).split('#')[0].strip()
                        val = ast.literal_eval(val_str)
                        config_values[key] = val
                    except (ValueError, SyntaxError):
                        config_values[key] = match.group(1).split('#')[0].strip().strip("'\"")
    except Exception as e:
        flash(f"Error reading config file: {e}", "danger")

    return {group: {key: config_values.get(key, '') for key in keys} for group, keys in CONFIG_SCHEMA.items()}

def save_config_values(new_values):
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            lines = f.readlines()

        with open(CONFIG_FILE_PATH, 'w') as f:
            for line in lines:
                content = line.strip()
                if not content or content.startswith('#'):
                    f.write(line)
                    continue

                match = re.match(r"^(\w+)\s*=", content)
                if match:
                    key = match.group(1)
                    if key in new_values:
                        comment = ''
                        if '#' in content:
                            comment_part = content.split('#', 1)[1]
                            comment = f'  # {comment_part.strip()}'

                        value_to_save = new_values[key]
                        # If the value is a float but has no decimal part, convert to int
                        if isinstance(value_to_save, float) and value_to_save.is_integer():
                            value_to_save = int(value_to_save)

                        new_value_repr = repr(value_to_save)
                        f.write(f'{key} = {new_value_repr}{comment}\n')
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
                            else:
                                typed_data[key] = value_str
                        except ValueError:
                            flash(f"Data type conversion failed for {key}. Aborting save.", "danger")
                            return redirect(url_for('index'))
            save_config_values(typed_data)
        return redirect(url_for('index'))

    config_data = get_config_values()
    return render_template('index.html', categorized_config=config_data, schema=CONFIG_SCHEMA, isinstance=isinstance, list=list)

if __name__ == '__main__':
    app.run(debug=True, port=5001)