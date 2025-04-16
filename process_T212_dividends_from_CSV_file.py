#!/usr/bin/env python3
import csv
import os
import re
import sys
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, InvalidOperation
from datetime import datetime, time

# Expected CSV headers
EXPECTED_HEADERS = [
    "Action", "Time", "ISIN", "Ticker", "Name", "No. of shares",
    "Price / share", "Currency (Price / share)", "Exchange rate", "Total",
    "Currency (Total)", "Withholding tax", "Currency (Withholding tax)"
]

# Required headers for processing
REQUIRED_HEADERS = [
    "Action", "Time", "ISIN", "Name", "No. of shares",
    "Price / share", "Currency (Price / share)", "Withholding tax",
    "Currency (Withholding tax)"
]

ALLOWED_MODES = ["nap-autopilot", "table", "sheet"]

# --- Utility functions ---

def parse_args():
    mode = "nap-autopilot"
    input_file = output_file = None
    # Allow mode to appear anywhere in the arguments.
    args = sys.argv[1:]
    for arg in args:
        if arg.startswith("mode="):
            mode = arg.split("=", 1)[1].strip()
        elif input_file is None:
            input_file = arg
        else:
            output_file = arg
    if not input_file or not output_file:
        print("Usage: ./process_T212_dividends.py input.csv output.csv [mode=nap-autopilot|sheet|table]")
        sys.exit(1)
    if mode not in ALLOWED_MODES:
        print(f"ERROR: Mode '{mode}' is not valid. Allowed modes are: {', '.join(ALLOWED_MODES)}")
        sys.exit(1)
    return input_file, output_file, mode

def read_csv_file(file_path):
    empty_lines = 0
    with open(file_path, newline='', encoding='utf-8') as f:
        content = f.read()
        empty_lines = len([l for l in content.splitlines() if not l.strip()])
        f.seek(0)
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        # Check for header order and exact match
        if headers != EXPECTED_HEADERS:
            unexpected = set(headers) - set(EXPECTED_HEADERS) if headers else set()
            missing = set(EXPECTED_HEADERS) - set(headers) if headers else set(EXPECTED_HEADERS)
            print(f"WARNING: Headers mismatch. Unexpected: {unexpected}. Missing: {missing}.")
        # Check required headers
        for h in REQUIRED_HEADERS:
            if h not in headers:
                print(f"ERROR: Missing required header: {h}")
                sys.exit(1)
        rows = list(reader)
    if empty_lines:
        print(f"WARNING: Found {empty_lines} empty lines in input file.")
    return headers, rows

def validate_action(row):
    action = row["Action"]
    if action == "Dividend (Dividend)":
        return True
    elif "dividend" in action.lower():
        print(f"WARNING: Action not exactly 'Dividend (Dividend)' but contains 'dividend': {action}")
        return True
    else:
        print(f"WARNING: Unrecognized action; ignoring row: {row}")
        return False

def check_currency_match(row):
    if row["Currency (Price / share)"] != row["Currency (Withholding tax)"]:
        print("ERROR: 'Currency (Price / share)' and 'Currency (Withholding tax)' do not match")
        sys.exit(1)

def convert_date(time_str):
    try:
        date_part = time_str[:10]
        time_part = time_str[11:]
        date_obj = datetime.strptime(date_part, "%Y-%m-%d")
        time_obj = datetime.strptime(time_part, "%H:%M:%S").time()
        if time_obj > time(20, 59, 59):
            print(f"WARNING: Time is after 20:59:59 ({time_part}), date might be different due to time zones.")
        return date_obj.strftime("%d.%m.%Y")
    except Exception as e:
        print(f"ERROR: Invalid date format or conversion failed: {e}")
        sys.exit(1)

def safe_decimal(value, field_name):
    try:
        if value.strip() == "":
            print(f"WARNING: Empty value for {field_name}, assuming 0.")
            return Decimal("0")
        return Decimal(value)
    except InvalidOperation:
        print(f"ERROR: Invalid numeric value for {field_name}: {value}")
        sys.exit(1)

def check_required_fields(row):
    isin = row["ISIN"]
    if not isin:
        print("ERROR: ISIN is empty")
        sys.exit(1)
    if not re.match(r'^[A-Z]{2}', isin):
        print(f"ERROR: First two symbols of ISIN not uppercase Latin: {isin}")
        sys.exit(1)
    if not row["Name"]:
        print("ERROR: Missing Name")
        sys.exit(1)
    if safe_decimal(row["No. of shares"], "No. of shares") == 0:
        print("ERROR: 'No. of shares' is zero or empty")
        sys.exit(1)
    if safe_decimal(row["Price / share"], "Price / share") == 0:
        print("ERROR: 'Price / share' is zero or empty")
        sys.exit(1)

def round_decimal(value, rounding=ROUND_HALF_UP):
    return value.quantize(Decimal("0.01"), rounding=rounding)

def ceil_cent(value):
    return (value * 100).to_integral_value(rounding=ROUND_CEILING) / Decimal("100")

def look_for_currency_rate(code, date_str):
    # date_str expected in DD.MM.YYYY format; extract year
    if code == "BGN":
        return Decimal("1")
    if code == "EUR":
        return Decimal("1.95583")
    year = date_str[-4:]
    filenames = [f"{code}_{year}_corrected.csv", f"{code}_{year}.csv", f"{code}.csv"]
    # Get directory of the script and build the path to the 'currency_rates' directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    currency_dir = os.path.join(script_dir, "currency_rates")
    paths_to_try = [os.path.join(currency_dir, fn) for fn in filenames] + filenames
    for path in paths_to_try:
        if os.path.isfile(path):
            with open(path, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                # If the first row doesn't look like a date (i.e., it's a header), skip it.
                if rows and not re.match(r'\d{2}\.\d{2}\.\d{4}', rows[0][0]):
                    rows = rows[1:]
                for r in rows:
                    if r and r[0] == date_str:
                        try:
                            return Decimal(r[1])
                        except InvalidOperation:
                            print(f"ERROR: Invalid exchange rate value in {path} for date {date_str}")
                            sys.exit(1)
                print(f"ERROR: Rate not found for {code} on {date_str} in {path}")
                sys.exit(1)
    print(f"ERROR: No currency rate file found for {code} among {filenames}")
    sys.exit(1)

def guess_the_country_tax_residence(isin):
    code = isin[:2]
    if not re.match(r'^[A-Z]{2}$', code):
        print(f"WARNING: ISIN prefix not valid: {code}")
        return "UNKNOWN"
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "ISIN_country.csv")
    if os.path.isfile(path):
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("ISIN") == isin:
                    return row.get("Country_tax", code)
    print(f"notice: Using fallback ISIN country code for {isin}: {code}")
    return code

# --- Main Processing ---
def process_rows(rows):
    processed = []
    unrecognized_count = 0
    for row in rows:
        # Validate action field; skip row if not acceptable.
        if not validate_action(row):
            unrecognized_count += 1
            continue

        check_currency_match(row)
        check_required_fields(row)

        # Convert and process date
        date_converted = convert_date(row["Time"])

        # Read numeric fields
        no_of_shares = safe_decimal(row["No. of shares"], "No. of shares")
        price_per_share = safe_decimal(row["Price / share"], "Price / share")
        withholding_tax = safe_decimal(row["Withholding tax"], "Withholding tax")
#        total_csv = safe_decimal(row["Total"], "Total") if row["Total"].strip() != "" else None
        
        # Convert amounts if currency is GBX: divide by 100 and update currency code to GBP.
        currency_code = row["Currency (Price / share)"]
        if currency_code.upper() == "GBX":
            price_per_share = price_per_share / Decimal("100")
            withholding_tax = withholding_tax / Decimal("100")
            currency_code = "GBP"

        # Compute gross dividend = (No. of shares * Price / share) + Withholding tax
        gross_dividend = no_of_shares * price_per_share + withholding_tax

        # Standard rounding (accounting rounding)
        gross_dividend_std = round_decimal(gross_dividend, ROUND_HALF_UP)
        # Alternative rounding: ceiling rounding to nearest cent
        gross_dividend_alt = ceil_cent(gross_dividend)

#        if gross_dividend_std != gross_dividend_alt:
#            diff = gross_dividend_alt - gross_dividend_std
#            print(f"notice: Rounding difference for {row['Ticker']} on {date_converted}: standard={gross_dividend_std}, ceil-rounded={gross_dividend_alt} (difference: {diff}).")

        # Look up currency rate for conversion
        curr_rate = look_for_currency_rate(currency_code, date_converted)

        # Make sure we don't under-declare taxes due to rounding errors

        # In some cases when the withholding tax is computed wrongly as
        # less than half cent we may get 0.00 on the gross dividend field.
        # To prevent this we assume minimum 0.01 dividend. This does not
        # impact taxes because 5% of less than 0.09 BGN is rounded to 0.
        # We can't declare a tax smaller than 0.01 BGN, technically it's not possible).
        
        # Note that errors due to gross rate not computed correctly because of
        # rounding errors on withholding taxes typically do not impact taxes
        # because withholding taxes are typically more than 5% and accourding to 
        # anti-double taxation rules we don't owe taxes in Bulgaria.
        
        # I can't think of other country that withholds taxes less than 5% (but over 0%).
        
        dividend_BGN_v1 = round_decimal(gross_dividend * curr_rate, ROUND_HALF_UP)
        dividend_BGN_v2 = round_decimal(gross_dividend_std * curr_rate, ROUND_HALF_UP)
        dividend_BGN     = max( dividend_BGN_v1, dividend_BGN_v2, Decimal("0.01") )

       #alt_dividend_BGN = round_decimal(gross_dividend_alt * curr_rate, ROUND_HALF_UP)
       #alt_dividend_BGN = ceil_cent(gross_dividend_alt * curr_rate)

        alt_dividend_BGN = max( ceil_cent(gross_dividend_alt * curr_rate) ,  Decimal("0.01") )

       #dividend_BGN_precise = round_decimal(gross_dividend * curr_rate, ROUND_HALF_UP)
       #alt_dividend_BGN_precise = ceil_cent(gross_dividend * curr_rate)

        paidtax_BGN = round_decimal(withholding_tax * curr_rate, ROUND_HALF_UP)


        #******* Preventing 0.00 dividends ******************************
        # old code, max( ... , 0.01) is simpler

#        if dividend_BGN < Decimal(0.05):
#        
#            total_csv_plus_paidtax_BGN = (total_csv + paidtax_BGN)
#            # If CSV Total is provided and Currency (Total) is BGN, compare with computed dividend_BGN.
#            if row["Currency (Total)"].upper() == "BGN" and total_csv is not None:
#                if dividend_BGN < total_csv_plus_paidtax_BGN:
#                    diff = abs(dividend_BGN - total_csv_plus_paidtax_BGN)
#                    print(f"WARNING: Computed dividend_BGN ({dividend_BGN}) and (CSV Total ({total_csv}) + paidtax_BGN ({paidtax_BGN})) do not make sense for {row['Ticker']}. Difference: {diff}. Dividend_BGN will be {total_csv_plus_paidtax_BGN}.")
#
#                    #dividend_BGN = alt_dividend_BGN
#                    dividend_BGN = total_csv_plus_paidtax_BGN
#
#                    # dividend_BGN = Decimal(20.79) # for testing/debugging

        # Compute tax credits and tax due (primary calculation)
        permitted_tax_credit = round_decimal(dividend_BGN * Decimal("0.05"), ROUND_HALF_UP)
        method = "1" if paidtax_BGN > Decimal("0") else "3"
        if paidtax_BGN == 0:
            tax_due = permitted_tax_credit
            permitted_tax_credit = 0 # for consistency with nap-autopilot
        else:
            tax_due = max(Decimal("0"), permitted_tax_credit - paidtax_BGN)

        if paidtax_BGN > 0:
            if paidtax_BGN < permitted_tax_credit:
                print(f"WARNING: Withheld tax on the dividend is less than 5% of the dividend for {row['Ticker']} on {date_converted}. This is unusual and may be due to rounding errors and the deficiencies in the CSV data.")
        
        # Alternative tax due calculation using alternative rounding for dividend
        permitted_tax_credit_alt = round_decimal(alt_dividend_BGN * Decimal("0.05"), ROUND_HALF_UP)
        if paidtax_BGN == 0:
            alt_tax_due = permitted_tax_credit_alt
        else:
            alt_tax_due = max(Decimal("0"), permitted_tax_credit_alt - paidtax_BGN)
        if tax_due != alt_tax_due:
            diff_tax = alt_tax_due - tax_due
            print(f"WARNING: Alternative tax due calculation for {row['Ticker']} on {date_converted} differs by {diff_tax} (primary: {tax_due}, alternative: {alt_tax_due}).")

        country = guess_the_country_tax_residence(row["ISIN"])
        name = row["Name"]
        if len(name) > 200:
            print(f"WARNING: Name too long for {row['Ticker']}. Truncating to 200 characters.")
            name = name[:200]

        processed.append({
            "name": name,
            "country": country,
            "no_of_shares": no_of_shares,
            "price_per_share": price_per_share,
            "withholding_tax": withholding_tax,
            #"gross_dividend": gross_dividend_std,
            "gross_dividend": gross_dividend, # no rounding
            "date": date_converted,
            "currency_code": currency_code,
            "curr_rate": curr_rate,
            "dividend_BGN": dividend_BGN,
            "paidtax_BGN": paidtax_BGN,
            "permitted_tax_credit": permitted_tax_credit,
            "method": method,
            "applied_tax_credit": min(paidtax_BGN, permitted_tax_credit),
            "tax_due": tax_due,
            "income_code": "8141"
        })
    if unrecognized_count:
        print(f"WARNING: Found {unrecognized_count} unrecognized lines in the file that were ignored.")
    return processed

def write_output(processed, output_file, mode):
    if mode == "nap-autopilot":
        headers = ["name", "country", "sum", "paidtax"]
        with open(output_file, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in processed:
                writer.writerow({
                    "name": row["name"],
                    "country": row["country"],
                    "sum": f"{row['dividend_BGN']:.2f}",
                    "paidtax": f"{row['paidtax_BGN']:.2f}"
                })
    elif mode == "sheet":
        headers = ["name", "currency code", "dividend", "withholding tax", "date", "currency rate",
                   "dividend BGN", "withholding tax BGN", "permitted tax credit", "method",
                   "applied tax credit", "tax due", "country"]
        with open(output_file, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in processed:
                writer.writerow({
                    "name": row["name"],
                    "currency code": row["currency_code"],
                    "dividend": f"{row['gross_dividend']:.2f}",
                    "withholding tax": f"{row['withholding_tax']:.2f}",
                    "date": row["date"],
                    "currency rate": f"{row['curr_rate']:.5f}",
                    "dividend BGN": f"{row['dividend_BGN']:.2f}",
                    "withholding tax BGN": f"{row['paidtax_BGN']:.2f}",
                    "permitted tax credit": f"{row['permitted_tax_credit']:.2f}",
                    "method": row["method"],
                    "applied tax credit": f"{row['applied_tax_credit']:.2f}",
                    "tax due": f"{row['tax_due']:.2f}",
                    "country": row["country"]
                })
    elif mode == "table":
        headers = ["name", "country", "income code", "method", "dividend BGN",
                   "withholding tax BGN", "permitted tax credit", "applied tax credit", "tax due"]
        with open(output_file, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in processed:
                writer.writerow({
                    "name": row["name"],
                    "country": row["country"],
                    "income code": row["income_code"],
                    "method": row["method"],
                    "dividend BGN": f"{row['dividend_BGN']:.2f}",
                    "withholding tax BGN": f"{row['paidtax_BGN']:.2f}",
                    "permitted tax credit": f"{row['permitted_tax_credit']:.2f}",
                    "applied tax credit": f"{row['applied_tax_credit']:.2f}",
                    "tax due": f"{row['tax_due']:.2f}"
                })
    print(f"Output written to {output_file} in mode '{mode}'.")

def main():
    input_file, output_file, mode = parse_args()

    # Check that the input file exists.
    if not os.path.isfile(input_file):
        print(f"ERROR: Input file {input_file} does not exist.")
        sys.exit(1)

    # Check that the output file does NOT already exist.
    if os.path.exists(output_file):
        print(f"ERROR: Output file {output_file} already exists. Aborting to prevent accidental overwrite.")
        sys.exit(1)

    headers, rows = read_csv_file(input_file)
    processed = process_rows(rows)
    write_output(processed, output_file, mode)

if __name__ == '__main__':
    main()

