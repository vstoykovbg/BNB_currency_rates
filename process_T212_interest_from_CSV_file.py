#!/usr/bin/env python3
import csv
import sys
import os
import re
from datetime import datetime, timedelta, time
from decimal import Decimal, getcontext, InvalidOperation, ROUND_HALF_UP

# Set global Decimal precision
getcontext().prec = 28

VALID_ACTION = "Interest on cash"
VALID_CURRENCIES = ["EUR", "BGN"]

def convert_date(time_str, currency):
    try:
        date_part = time_str[:10]
        time_part = time_str[11:]
        date_obj = datetime.strptime(date_part, "%Y-%m-%d")
        time_obj = datetime.strptime(time_part, "%H:%M:%S").time()
        dt_utc = datetime.combine(date_obj, time_obj)
        dt_bulgaria = dt_utc + timedelta(hours=3)

        if dt_utc.time() >= time(20, 59, 59):
            if (currency not in VALID_CURRENCIES) or (date_obj.month == 12 and date_obj.day == 31):
                print(f"WARNING: UTC time {dt_utc.strftime('%Y-%m-%d %H:%M:%S')} is at/after 20:59:59;")
                print(f"         local Bulgaria time: {dt_bulgaria.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"         (date is changed if we assume the offset is 3 hours)")
                print(f"         POSSIBLY THE SCRIPT IS USING A WRONG DATE FOR THE CURRENCY RATE")
        return date_obj.strftime("%d.%m.%Y")
    except Exception as e:
        print(f"ERROR: Invalid date format or conversion failed: {e}")
        sys.exit(1)

def look_for_currency_rate(code, date_str):
    if code == "BGN":
        return Decimal("1")
    if code == "EUR":
        return Decimal("1.95583")
    year = date_str[-4:]
    filenames = [f"{code}_{year}_corrected.csv", f"{code}_{year}.csv", f"{code}.csv"]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    currency_dir = os.path.join(script_dir, "currency_rates")
    paths_to_try = [os.path.join(currency_dir, fn) for fn in filenames] + filenames
    for path in paths_to_try:
        if os.path.isfile(path):
            with open(path, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
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

def print_usage():
    script_name = os.path.basename(sys.argv[0])
    print(f"Usage:  {script_name} [mode=sheet|total] <input_csv> [output_csv]")
    print("  mode         Optional. 'sheet' to generate spreadsheet-style output, 'total' (default) for summary.")
    print("  input_csv    Required. Path to the input CSV file.")
    print("  output_csv   Optional. Path to the output CSV file.")

def parse_arguments(args):
    mode = "total"
    input_file = None
    output_file = None
    remaining = []
    
    for arg in args:
        if arg.startswith("mode="):
            mode = arg.split("=", 1)[1].strip().lower()
        else:
            remaining.append(arg)
    
    args = remaining
    
    if len(args) < 1:
        print("ERROR: Input CSV file is required.\n")
        print_usage()
        sys.exit(1)
    
    input_file = args[0]
    
    if len(args) > 1:
        output_file = args[1]
    
    if mode not in ("sheet", "total"):
        print(f"ERROR: Invalid mode '{mode}'. Mode must be 'sheet' or 'total'.\n")
        print_usage()
        sys.exit(1)
    
    return input_file, output_file, mode

def process_csv(input_filename, output_filename=None, mode="total"):
    try:
        if output_filename and os.path.exists(output_filename):
            print(f"ERROR: Output file {output_filename} already exists. Aborting to prevent overwrite.")
            sys.exit(1)

        with open(input_filename, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            required_columns = ["Action", "Time", "Currency (Total)", "Total"]
            for col in required_columns:
                if col not in reader.fieldnames:
                    print(f"ERROR: Missing required column: {col}")
                    sys.exit(1)

            processed_rows = []
            total_less = Decimal("0.00")
            total_more = Decimal("0.00")

            for row in reader:
                if row["Action"] != VALID_ACTION:
                    print(f"WARNING: Skipping row with unexpected action: {row['Action']} (expected: {VALID_ACTION})")
                    continue
                try:
                    currency = row["Currency (Total)"].strip()
                    amount = Decimal(row["Total"])
                    date_str = convert_date(row["Time"], currency)
                    rate = look_for_currency_rate(currency, date_str)
                    bgn_less = (amount * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    bgn_more = (amount * rate).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

                    total_less += bgn_less
                    total_more += bgn_more

                    processed_rows.append({
                        "date": date_str,
                        "amount": amount,
                        "currency": currency,
                        "rate": rate,
                        "bgn_less": bgn_less,
                        "bgn_more": bgn_more
                    })
                except Exception as e:
                    print(f"ERROR processing row: {row}")
                    print(str(e))
                    sys.exit(1)

            print(f"Total (rounded to 0.01): {total_less}")
            print(f"Total (more precise, rounded to 0.000001): {total_more}")
            total_more_rounded = total_more.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            print(f"The above number rounded to 0.01: {total_more_rounded}")

            accepted_total = max(total_less, total_more_rounded)
            print(f"Accepted final total (larger value): {accepted_total}")

            if mode == "sheet":
                if not output_filename:
                    print("ERROR: Output file required for sheet mode.")
                    sys.exit(1)
                with open(output_filename, "w", newline='', encoding='utf-8') as out_file:
                    writer = csv.writer(out_file)
                    writer.writerow(["Date", "Value in currency", "Currency code", "Currency rate", "Value in BGN (0.01)", "Value in BGN (0.000001)"])
                    for r in processed_rows:
                        writer.writerow([
                            r["date"],
                            r["amount"],
                            r["currency"],
                            r["rate"],
                            r["bgn_less"],
                            r["bgn_more"]
                        ])
                    print(f"Sheet saved to: {output_filename}")
            elif mode == "total" and output_filename:
                with open(output_filename, "w", encoding='utf-8') as out_file:
                    out_file.write(f"{accepted_total}\n")

            print(f"💰 Total BGN: {accepted_total}")

    except Exception as e:
        print(f"ERROR: Failed to process CSV: {e}")
        sys.exit(1)

def main():
    input_file, output_file, mode = parse_arguments(sys.argv[1:])
    process_csv(input_file, output_file, mode)

if __name__ == "__main__":
    main()

