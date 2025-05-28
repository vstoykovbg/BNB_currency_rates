#!/usr/bin/env python3
import csv
import sys
import os
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from datetime import datetime
from collections import Counter

ALLOWED_MODES = ["nap-autopilot", "table", "sheet"]

duplicate_warning_count = 0
unknown_isin_count = 0
orphan_tax_warning_count = 0
duplicate_tax_warning_count = 0
identical_tax_warning_count = 0
positive_tax_warning_count = 0
duplicate_dividend_count = 0
duplicate_records = []
unusual_tax_warning_count = 0
very_unusual_tax_warning_count = 0
unusually_low_tax_warning_count = 0

def parse_args():
    args = sys.argv[1:]

    if not args:
        print(f"Usage:")
        print(f"\nSingle file mode:\n")
        print(f"  {os.path.basename(sys.argv[0])} input.csv output.csv [mode={'|'.join(ALLOWED_MODES)}] [input=one]")
        print(f"\nTwo files mode (with separate Financial Instrument Information):\n")
        print(f"  {os.path.basename(sys.argv[0])} input.csv input_FII.csv output.csv [mode={'|'.join(ALLOWED_MODES)}] [input=two]")
        print(f"\nOptions:\n")
        print(f"  mode=      Output format: {', '.join(ALLOWED_MODES)}")
        print(f"  input=     Input mode: 'one' (single file) or 'two' (separate FII file)")
        sys.exit(1)

    mode = ALLOWED_MODES[0]
    input_mode = "one"  # Default to single input file
    input_file = fii_file = output_file = None

    # Separate arguments
    positional_args = []
    for arg in args:
        if arg.startswith("mode="):
            mode = arg.split("=", 1)[1].strip()
        elif arg.startswith("input="):
            input_mode = arg.split("=", 1)[1].strip()
        elif "=" in arg:
            print(f"ERROR: Unexpected argument '{arg}'. Only mode= and input= are supported.")
            sys.exit(1)
        else:
            positional_args.append(arg)

    # Validate input mode
    if input_mode not in ("one", "two"):
        print("ERROR: input= must be either 'one' or 'two'")
        sys.exit(1)

    # Validate filenames count based on input mode
    if input_mode == "one" and len(positional_args) != 2:
        print(f"Usage (single file): {os.path.basename(sys.argv[0])} input.csv output.csv [mode={'|'.join(ALLOWED_MODES)}] [input=one]")
        sys.exit(1)
    elif input_mode == "two" and len(positional_args) != 3:
        print(f"Usage (two files): {os.path.basename(sys.argv[0])} input.csv input_FII.csv output.csv [mode={'|'.join(ALLOWED_MODES)}] [input=two]")
        sys.exit(1)

    # Assign files based on input mode
    if input_mode == "one":
        input_file, output_file = positional_args
    else:
        input_file, fii_file, output_file = positional_args

    # Validate extensions
    for f in [input_file, fii_file, output_file]:
        if f and not f.lower().endswith(".csv"):
            print(f"ERROR: All files must have .csv extension. Found: {f}")
            sys.exit(1)

    # Validate mode
    if mode not in ALLOWED_MODES:
        print(f"ERROR: Invalid mode '{mode}'. Allowed modes: {', '.join(ALLOWED_MODES)}")
        sys.exit(1)

    return {
        'input_file': input_file,
        'fii_file': fii_file if input_mode == "two" else None,
        'output_file': output_file,
        'mode': mode,
        'input_mode': input_mode
    }

def extract_instrument_names(rows):
    isin_name_map = {}
    headers = {}
    headers_security_id = None
    headers_description = None
    in_fii_section = False

    for row in rows:
        if not row or len(row) < 4:
            continue
        
        section, row_type = row[0], row[1]

        if section == "Financial Instrument Information":
            if row_type == "Header":
                current_headers = {key: idx for idx, key in enumerate(row[2:], start=2)}
                
                # Check if this header has the required columns
                if "Description" in current_headers and "Security ID" in current_headers:
                    # This is a compatible header, use it
                    headers = current_headers
                    headers_description = headers["Description"]
                    headers_security_id = headers["Security ID"]
                    in_fii_section = True
                else:
                    # This header doesn't have what we need, skip it
                    in_fii_section = False
                    continue

            elif row_type == "Data" and in_fii_section:
                try:
                    isin = row[headers_security_id].strip()
                    name = row[headers_description].strip()
                    if isin and name:
                        isin_name_map[isin] = name
                except IndexError:
                    continue
        elif in_fii_section and section != "Financial Instrument Information":
            # We've left the section we were processing
            in_fii_section = False

    return isin_name_map

def guess_the_country_tax_residence(isin):
    global unknown_isin_count
    code = isin[:2]
    if not re.match(r'^[A-Z]{2}$', code):
        print(f"WARNING: ISIN prefix not valid: {code}")
        unknown_isin_count += 1
        return "UNKNOWN"
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

def check_withholding_tax_rate(dividend_amount, tax_amount, country_code, isin, ticker):
    """
    Check if withholding tax is unusually high or low for the given country.
    Returns tuple: (is_high_warning, is_low_warning)
    """
    global unusual_tax_warning_count, very_unusual_tax_warning_count, unusually_low_tax_warning_count
    
    # Initialize thresholds
    very_unusual_high_threshold = Decimal('0.50')  # 50%
    rounding_buffer = Decimal('0.01')  # 1 cent buffer for rounding tolerance
    
    if dividend_amount <= 0 or tax_amount >= 0:
        return False, False  # Skip invalid cases

    abs_tax = abs(tax_amount)
    exact_rate = abs_tax / dividend_amount
    
    # Calculate buffered rates (corrected directions)
    rate_high_test = (abs_tax - rounding_buffer) / dividend_amount  # More lenient for high check
    rate_low_test = (abs_tax + rounding_buffer) / dividend_amount   # More lenient for low check

    country_code = country_code.upper() if country_code else "UNKNOWN"
    
    # Define expected tax rates by country
    tax_rates = {
        # Format: {'max': max expected rate, 'min': min expected rate (None for zero-tax)}
        'US': {'max': Decimal('0.10'), 'min': Decimal('0.10')},    # Exact 10%
        'CA': {'max': Decimal('0.15'), 'min': Decimal('0.15')},    # Exact 15%
        'DE': {'max': Decimal('0.26375'), 'min': Decimal('0.26375')},
        'FR': {'max': Decimal('0.30'), 'min': Decimal('0.12')},
        # Zero-tax jurisdictions
        'UK': {'max': Decimal('0'), 'min': None},
        'GB': {'max': Decimal('0'), 'min': None},
        'KY': {'max': Decimal('0'), 'min': None},
        'VG': {'max': Decimal('0'), 'min': None},
        'JE': {'max': Decimal('0'), 'min': None},  # Jersey
        'GG': {'max': Decimal('0'), 'min': None},  # Guernsey
    }
    
    thresholds = tax_rates.get(country_code, {'max': Decimal('0.30'), 'min': None})
    max_rate = thresholds['max']
    min_rate = thresholds['min']

    # High tax check (using more lenient rate_high_test)
    high_warning = False
    if exact_rate > very_unusual_high_threshold:
        very_unusual_tax_warning_count += 1
        print(f"🚨  VERY HIGH TAX: {isin} {ticker} ({country_code}):")
        high_warning = True
    elif rate_high_test > max_rate:  # Only warn if above threshold even after being more lenient
        print(f"⚠️  High tax: {isin} {ticker} ({country_code}):")
        high_warning = True

    if high_warning:
        unusual_tax_warning_count += 1
        print(f"  • Dividend: {dividend_amount:.2f}")
        print(f"  • Withheld: {tax_amount:.2f}")
        print(f"  • Rate: {exact_rate*100:.6f}% (Max expected: {max_rate*100:.2f}%)")

    # Low tax check (using more lenient rate_low_test)
    low_warning = False
    if min_rate is not None and rate_low_test < min_rate:  # Only warn if below threshold after being more lenient
        unusually_low_tax_warning_count += 1
        print(f"⚠️  Low tax: {isin} {ticker} ({country_code}):")
        print(f"  • Dividend: {dividend_amount:.2f}")
        print(f"  • Withheld: {tax_amount:.2f}")
        print(f"  • Rate: {exact_rate*100:.6f}% (Min expected: {min_rate*100:.2f}%)")
        low_warning = True
    
    return high_warning, low_warning

def round_decimal(value, rounding=ROUND_HALF_UP):
    return value.quantize(Decimal("0.01"), rounding=rounding)

def look_for_currency_rate(code, date_str):
    # date_str expected in DD.MM.YYYY format
    code = code.upper()
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
            try:
                with open(path, newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    rows = list(reader)

                    # Skip header if needed
                    if rows and not re.match(r'\d{2}\.\d{2}\.\d{4}', rows[0][0]):
                        rows = rows[1:]

                    for r in rows:
                        if r and r[0] == date_str:
                            try:
                                return Decimal(r[1])
                            except InvalidOperation:
                                print(f"ERROR: Invalid exchange rate value '{r[1]}' in {path} for date {date_str}")
                                sys.exit(1)
                            except Exception as e:
                                print(f"ERROR: Unexpected issue converting exchange rate in {path} on {date_str}: {e}")
                                sys.exit(1)

                    print(f"ERROR: Rate not found for {code} on {date_str} in {path}")
                    sys.exit(1)

            except Exception as e:
                print(f"ERROR: Failed to read currency file '{path}': {e}")
                sys.exit(1)

    print(f"ERROR: No currency rate file found for {code} among {filenames}")
    sys.exit(1)

def extract_base_desc(desc):
    """
    Extract a base description for matching.
    For dividends, take text up to the first " (".
    For taxes, take text up to " -".
    This ensures the ticker, ISIN, and dividend detail are part of the key.
    """
    if " -" in desc:
        base = desc.split(" -")[0].strip()
    elif " (" in desc:
        base = desc.split(" (")[0].strip()
    else:
        base = desc.strip()
    print(f"DEBUG: extract_base_desc: Original: '{desc}' -> Base: '{base}'")
    return base

def extract_dividends_and_taxes(rows):
    dividends = []
    taxes = []
    seen_dividends = set()
    seen_taxes = set()
    global duplicate_warning_count, unknown_isin_count, duplicate_tax_warning_count

    div_col_map = {}
    tax_col_map = {}

    for i, row in enumerate(rows):
        print(f"DEBUG: Processing row {i}: {row}")

        if len(row) < 6:
            continue  # Skip short or malformed rows

        section = row[0]
        row_type = row[1]

        # Build column map for dividends
        if section == "Dividends" and row_type == "Header":
            div_col_map = {col.strip(): idx + 2 for idx, col in enumerate(row[2:])}
            continue

        # Build column map for taxes
        if section == "Withholding Tax" and row_type == "Header":
            tax_col_map = {col.strip(): idx + 2 for idx, col in enumerate(row[2:])}
            continue

        # Handle dividend data
        if section == "Dividends" and row_type == "Data":
            if not div_col_map:
                print("ERROR: No Dividends header found before data rows.")
                continue

            try:
                desc = row[div_col_map["Description"]].strip()
                amount_str = row[div_col_map["Amount"]].strip()
                date_str = row[div_col_map["Date"]].strip()
                currency = row[div_col_map["Currency"]].strip()
            except (KeyError, IndexError):
                print("ERROR: Missing expected columns in Dividends row.")
                continue

            if not date_str or not desc:
                print("DEBUG: skipping the line because not date_str or not desc")
                continue

            ticker_match = re.match(r'(.*?)\((.*?)\)', desc)
            if not ticker_match:
                continue

            ticker, isin = ticker_match.group(1).strip(), ticker_match.group(2).strip()
            base_desc = extract_base_desc(desc)

            try:
                div_date = datetime.strptime(date_str, "%Y-%m-%d")
                amount = Decimal(amount_str)
            except Exception as e:
                print(f"Skipping malformed row: {e}")
                continue

            key = (div_date, isin, ticker, amount)

            if key in seen_dividends:
                duplicate_warning_count += 1
                print(f"WARNING: Duplicate dividend detected for {ticker} on {div_date.date()} amount {amount}")
                print("  ⚠️  This will cause incorrect tax calculations - output may be invalid")
                # Create a detailed duplicate record
                duplicate_records.append({
                    'date': div_date,
                    'isin': isin,
                    'ticker': ticker,
                    'amount': amount,
                    'currency': currency,
                    'desc': base_desc
                })

            seen_dividends.add(key)

            print(f"DEBUG: Dividend row: Ticker: {ticker}, Date: {div_date.date()}, Base desc: '{base_desc}', Amount: {amount}")
            dividends.append({
                "date": div_date,
                "desc": base_desc,
                "amount": amount,
                "currency": currency,
                "isin": isin,
                "ticker": ticker,
                "country": guess_the_country_tax_residence(isin)
            })

        # Handle tax data
        elif section == "Withholding Tax" and row_type == "Data":
            if not tax_col_map:
                print("ERROR: No Withholding Tax header found before data rows.")
                continue

            try:
                desc = row[tax_col_map["Description"]].strip()
                amount_str = row[tax_col_map["Amount"]].strip()
                date_str = row[tax_col_map["Date"]].strip()
            except (KeyError, IndexError):
                print("ERROR: Missing expected columns in Withholding Tax row.")
                continue

            if not date_str or not amount_str or "Interest" in desc:
                continue

            base_desc = extract_base_desc(desc)
            try:
                tax_date = datetime.strptime(date_str, "%Y-%m-%d")
                amount = Decimal(amount_str)
            except Exception as e:
                print(f"Skipping malformed tax row: {e}")
                continue

            key = (tax_date, base_desc, amount)
            if key in seen_taxes:
                duplicate_tax_warning_count += 1
                print(f"WARNING: Duplicate withholding tax detected on {tax_date.date()} with description '{base_desc}' and amount {amount}")
            seen_taxes.add(key)

            print(f"DEBUG: Tax row: Date: {tax_date.date()}, Base desc: '{base_desc}', Amount: {amount}")
            taxes.append({
                "date": tax_date,
                "desc": base_desc,
                "amount": amount,
                "used": False
            })

    return dividends, taxes

def extract_from_flexquery(rows):
    global duplicate_tax_warning_count
    global duplicate_warning_count
    header = rows[0]
    idx = { name: i for i, name in enumerate(header) }

    dividends = []
    taxes     = []
    seen_divs = set()
    seen_taxs = set()

    for row in rows[1:]:
        if len(row) < len(header):
            continue

        typ = row[idx.get("Type", "")].strip()
        if typ not in ("Dividends", "Payment In Lieu Of Dividends", "Withholding Tax"):
            continue

        try:
            amt = Decimal(row[idx["Amount"]].strip())
            date_str = row[idx["SettleDate"]].strip()
            date = datetime.strptime(date_str, "%Y%m%d") if len(date_str) == 8 else datetime.strptime(date_str, "%Y-%m-%d")
            desc = row[idx["Description"]].strip()
            isin = row[idx["ISIN"]].strip()
            curr = row[idx["CurrencyPrimary"]].strip()
            country = row[idx["IssuerCountryCode"]].strip()
            ticker = row[idx["Symbol"]].strip() if "Symbol" in idx else "UNKNOWN"
            
            # --- Add ActionID extraction here ---
            action_id = row[idx["ActionID"]].strip() if ("ActionID" in idx and row[idx["ActionID"]].strip()) else None
        except Exception as e:
            print(f"Skipping malformed FlexQuery row: {e}")
            continue

        # --- Add ActionID to the base record ---
        record = dict(
            date=date,
            amount=amt,
            desc=extract_base_desc(desc),
            isin=isin,
            currency=curr,
            country=country,
            ticker=ticker,
            action_id=action_id
        )

        if typ in ("Dividends", "Payment In Lieu Of Dividends"):
            key = (date, isin, amt)
            if key in seen_divs:
                duplicate_warning_count += 1
                #print(f"WARNING: Duplicate dividend detected on {date.date()} with with description '{desc}' and amount {amt}")
                print(f"WARNING: Duplicate dividend detected for {ticker} on {date.date()} amount {amt}")
                print("  ⚠️  This will cause incorrect tax calculations - output may be invalid")
            seen_divs.add(key)
            dividends.append(record)

        elif typ == "Withholding Tax":
            # Skip interest-related tax rows with no symbol
            if not ticker and ( desc.upper().startswith("WITHHOLDING") or desc.upper().startswith("CANCEL WITHHOLDING ON CREDIT INT") ):
                print(f"DEBUG: Skipping interest-related tax row: {desc}")
                continue

            key = (date, desc, amt)
            if key in seen_taxs:
                duplicate_tax_warning_count += 1
                print(f"WARNING: Duplicate withholding tax detected on {date.date()} with description '{desc}' and amount {amt}")
            seen_taxs.add(key)
            record["used"] = False
            taxes.append(record)

    return dividends, taxes

def match_taxes_to_dividends(dividends, taxes):
    global orphan_tax_warning_count
    results = []
    
    for div in dividends:
        div_action_id = div.get("action_id")
        selected_taxes = []
        total_tax = Decimal("0")
        
        # METHOD 1: ActionID matching (FlexQuery only)
        actionid_taxes = []
        if div_action_id is not None:
            actionid_taxes = [t for t in taxes 
                            if not t["used"] 
                            and t.get("action_id") == div_action_id]
        
        # METHOD 2: Description/Date matching (always available)
        descdate_taxes = [t for t in taxes 
                         if not t["used"] 
                         and t["desc"] == div["desc"] 
                         and t["date"] == div["date"]]
        
        # STRICT COMPARISON (FlexQuery only)
        if div_action_id is not None:
            sum_actionid = sum(t["amount"] for t in actionid_taxes)
            sum_descdate = sum(t["amount"] for t in descdate_taxes)
            
            if sum_actionid != sum_descdate:  # Any difference triggers warning
                print(f"\nWARNING: Tax calculation mismatch for {div['ticker']} ({div['date'].date()})")
                print(f"  ActionID={div_action_id}: {sum_actionid}")
                print(f"  Desc/Date: {sum_descdate}")
                print(f"  Difference: {sum_actionid - sum_descdate}")
                
                # Detailed breakdown
                if actionid_taxes:
                    print("\n  ActionID-matched taxes:")
                    for t in actionid_taxes:
                        print(f"    {t['amount']} (ID: {t.get('action_id')} | {t['desc']})")
                
                if descdate_taxes and not actionid_taxes:
                    print("\n  Desc/Date-matched taxes:")
                    for t in descdate_taxes:
                        print(f"    {t['amount']} (ID: {t.get('action_id', 'None')} | {t['desc']})")
                print()
            
            # Prioritize ActionID matches if they exist
            selected_taxes = actionid_taxes if actionid_taxes else descdate_taxes
        else:
            # Legacy mode (Activity Statements)
            selected_taxes = descdate_taxes
        
        # PROCESS SELECTED TAXES
        if selected_taxes:
            for t in selected_taxes:
                t["used"] = True
            total_tax = sum(t["amount"] for t in selected_taxes)

            # NEW: Check for unusually high withholding tax
            if total_tax < 0:  # Only check if there's actual tax
                check_withholding_tax_rate(div["amount"], total_tax, div.get("country"), div.get("isin"), div["ticker"] )
            
            # NEW: Explicit logging for amended filings
            if len(selected_taxes) > 1:
                print(f"Note: {len(selected_taxes)} tax adjustments for {div['ticker']} on {div['date'].date()}:")
                for t in selected_taxes:
                    print(f"  {t['amount']} (ID: {t.get('action_id','N/A')})")
                print(f"  Net tax: {total_tax}")

                # Check if all adjustments are identical (likely duplicate)
                if len(set(t['amount'] for t in selected_taxes)) == 1:
                    print("  All adjustments are identical - indication of duplicate data in file")
                
                # Even stronger check - identical date+amount pairs (regardless of description/action_id)
                date_amount_pairs = [(t['date'], t['amount']) for t in selected_taxes]
                if len(date_amount_pairs) != len(set(date_amount_pairs)):
                    dup_count = len(date_amount_pairs) - len(set(date_amount_pairs))
                    print(f"  🚨  WARNING: {dup_count} duplicate date+amount pairs found - definite duplicate lines in file")
                    global identical_tax_warning_count
                    identical_tax_warning_count += dup_count

            method_used = ("ActionID" if (div_action_id and selected_taxes 
                          and selected_taxes[0].get("action_id") == div_action_id)
                          else "desc/date")
            print(f"DEBUG: Matched {len(selected_taxes)} tax(es) to {div['ticker']} "
                  f"on {div['date'].date()} using {method_used}")
        else:
            print(f"notice: No tax found for {div['ticker']} on {div['date'].date()}")
        
        # BUILD RESULT
        results.append({
            "name": div["ticker"],
            "isin": div["isin"],
            "date": div["date"],
            "currency": div["currency"],
            "gross": div["amount"],
            "withholding_tax": total_tax,
            "country": div["country"]  # Use pre-computed country
        })
    
    # ORPHAN TAX CHECK (with interest filtering)
    for tax in taxes:
        if (not tax["used"] and 
            not tax.get("desc", "").upper().startswith(("WITHHOLDING", "CANCEL WITHHOLDING ON CREDIT INT"))):
            orphan_tax_warning_count += 1
            print(f"\nWarning: Orphan tax detected (not matched to any dividend):")
            print(f"  Amount: {tax['amount']}")
            print(f"  Date: {tax['date'].date()}")
            print(f"  Description: {tax['desc']}")
            if tax.get("action_id"):
                print(f"  ActionID: {tax['action_id']}")
    
    return results

def convert_result_fields(results):
    global positive_tax_warning_count
    converted = []
    for r in results:
        # Handle currency conversion adjustments:
        currency = r["currency"].upper()
        gross = r["gross"]
        tax_total_non_positive=r["withholding_tax"]

        # Use the absolute value for tax calculations so that taxes are positive in the output.
        #tax_total = abs(tax_total_non_positive)

        # Convert negative tax values (expected in input) to positive (required for output)
        # If input tax is positive (unexpected), this will make it negative, clearly showing error
        tax_total = -tax_total_non_positive

        # If currency is GBX, convert to GBP.
        if currency == "GBX":
            gross = gross / Decimal("100")
            tax_total = tax_total / Decimal("100")
            currency = "GBP"

        # Format the date for currency rate lookup (DD.MM.YYYY)
        date_str = r["date"].strftime("%d.%m.%Y")
        curr_rate = look_for_currency_rate(currency, date_str)
        dividend_BGN = round_decimal(gross * curr_rate)
        if dividend_BGN < Decimal("0.01"):
            dividend_BGN = Decimal("0.01")
        paidtax_BGN = round_decimal(tax_total * curr_rate)

        permitted_tax_credit = round_decimal(dividend_BGN * Decimal("0.05"))

        if paidtax_BGN == Decimal("0"):
            tax_due = permitted_tax_credit
        else:
            tax_due = max(Decimal("0"), permitted_tax_credit - paidtax_BGN)
        applied_tax_credit = min(paidtax_BGN, permitted_tax_credit)

        if paidtax_BGN == Decimal("0"):
            method = "3"
            permitted_tax_credit = 0 # for consistency with nap-autopilot
        else:
            method = "1"

        if tax_total_non_positive > 0:
            positive_tax_warning_count += 1
            isin=r["isin"]
            print(f"WARNING: withholding tax for {isin} on {date_str} is positive ({tax_total_non_positive}), we expected negative or zero.")

        # Build result dictionary using the exact key names from your reference.
        new_r = {
            "name": r["name"],
            "ISIN": r["isin"],
            "country": r["country"],
            "dividend": f"{gross:.2f}",
            "withholding tax": f"{tax_total:.2f}",
            "date": date_str,
            "currency code": currency,
            "currency rate": f"{curr_rate:.5f}",
            "dividend BGN": f"{dividend_BGN:.2f}",
            "withholding tax BGN": f"{paidtax_BGN:.2f}",
            "permitted tax credit": f"{permitted_tax_credit:.2f}",
            "method": method,
            "applied tax credit": f"{applied_tax_credit:.2f}",
            "tax due": f"{tax_due:.2f}",
            "income code": "8141",
            "sum": f"{dividend_BGN:.2f}",
            "paidtax": f"{paidtax_BGN:.2f}"
        }
        converted.append(new_r)
    return converted

def read_ibkr_csv(file_path):
    with open(file_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        return list(reader)

def write_output(results, output_file, mode):
    with open(output_file, "w", newline='', encoding='utf-8') as f:
        if mode == "nap-autopilot":
            fieldnames = ["name", "country", "sum", "paidtax"]
        elif mode == "sheet":
            fieldnames = ["name", "ISIN", "currency code", "dividend", "withholding tax", "date",
                          "currency rate", "dividend BGN", "withholding tax BGN", "permitted tax credit",
                          "method", "applied tax credit", "tax due", "country"]
        elif mode == "table":
            fieldnames = ["name", "country", "income code", "method", "dividend BGN",
                          "withholding tax BGN", "permitted tax credit", "applied tax credit", "tax due"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            # Filter the result to only keys present in fieldnames.
            filtered = { key: r[key] for key in fieldnames if key in r }
            writer.writerow(filtered)
    print(f"ⓘ  notice: Output written to {output_file} in mode '{mode}'.")

def detect_duplicate_sections(rows):
    """Check for duplicate sections by counting header rows of the same section,
    while ignoring duplicate 'Financial Instrument Information' sections."""
    section_counts = Counter()
    
    for row in rows:
        # Only look at properly formatted header rows
        if len(row) >= 2 and row[1] == "Header":
            section_name = row[0]
            # Skip counting Financial Instrument Information sections
            if section_name != "Financial Instrument Information":
                section_counts[section_name] += 1
    
    # Return only sections that have more than one header row
    return [section for section, count in section_counts.items() if count > 1]

def analyze_duplicate_dividends(duplicates):
    """Generate detailed analysis of duplicate dividends"""
    if not duplicates:
        return
    
    print("\n🔍 Duplicate Dividend Analysis:")
    
    # Group by ISIN and date
    from collections import defaultdict
    dup_groups = defaultdict(list)
    for d in duplicates:
        dup_groups[(d['isin'], d['date'])].append(d)
    
    for (isin, date), entries in dup_groups.items():
        print(f"\n{isin} on {date.date()}: {len(entries)} duplicates")
        for i, entry in enumerate(entries, 1):
            print(f"  Duplicate {i}:")
            print(f"    Ticker: {entry['ticker']}")
            print(f"    Amount: {entry['amount']} {entry['currency']}")
            print(f"    Desc: {entry['desc']}")
    
    print("\n🚨 WARNING: Duplicate dividends will cause incorrect tax calculations")
    print("            The output should not be used for tax reporting")

def main():
    args = parse_args()  # Get config dictionary
    invalid_output = False
    input_is_FlexQuery = None
    serious_issues = False
    
    # Access parameters with clear names
    input_file = args['input_file']
    fii_file = args['fii_file']  # None in single-file mode
    output_file = args['output_file']
    mode = args['mode']
    
    # Check that the input file exists.
    if not os.path.isfile(input_file):
        print(f"ERROR: Input file {input_file} does not exist.")
        sys.exit(1)

    if args['input_mode'] == "two":

        # Check that the input file exists.
        if not os.path.isfile(fii_file):
            print(f"ERROR: Input file {fii_file} does not exist.")
            sys.exit(1)

    # Check that the output file does NOT already exist.
    if os.path.exists(output_file):
        print(f"ERROR: Output file {output_file} already exists. Aborting to prevent accidental overwrite.")
        sys.exit(1)

    raw_rows = read_ibkr_csv(input_file)

    if input_is_FlexQuery:
        duplicate_sections = None
    else:    
        duplicate_sections = detect_duplicate_sections(raw_rows)
        if duplicate_sections:
            print(f"\nWARNING: Input file contains duplicate sections ({', '.join(duplicate_sections)})")

    header = raw_rows[0]

    isin_name_map = {}

    if args['input_mode'] == "two":
        isin_name_map = extract_instrument_names(read_ibkr_csv(fii_file))

    if set(("CurrencyPrimary", "ISIN", "SettleDate")) <= set(header):
        print("notice: Input file detected as FlexQuery format.")
        input_is_FlexQuery = True
        dividends, taxes = extract_from_flexquery(raw_rows)
    else:
        # fallback to activity statement
        input_is_FlexQuery = False
        dividends, taxes = extract_dividends_and_taxes(raw_rows)
        if args['input_mode'] == "one":
            isin_name_map = extract_instrument_names(raw_rows)


    # --- Tax Year Detection & Filtering ---
    years = [d["date"].year for d in dividends]
    year_counts = Counter(years)
    if not year_counts:
        print("ERROR: No dividend data found.")
        sys.exit(1)

    max_count = max(year_counts.values())
    likely_years = [year for year, count in year_counts.items() if count == max_count]
    tax_year = max(likely_years)  # pick the latest in case of tie

    total_divs = len(dividends)
    ignored_divs = [d for d in dividends if d["date"].year != tax_year]
    kept_divs = [d for d in dividends if d["date"].year == tax_year]

    print(f"notice: Detected tax year: {tax_year} (based on {len(kept_divs)}/{total_divs} dividends)")

    for d in ignored_divs:
        print(f"notice: Ignoring dividend for {d['ticker']} on {d['date'].date()} (year {d['date'].year})")

    dividends = kept_divs

    # Store original count
    original_tax_count = len(taxes)

    # Filter tax rows to only match the detected tax year
    taxes = [t for t in taxes if t["date"].year == tax_year]

    # Calculate how many were removed
    removed = original_tax_count - len(taxes)
    if removed > 0:
        print(f"notice: Removed {removed} tax row(s) outside tax year {tax_year}")


    # --- End Tax Year Check ---

    matched_results = match_taxes_to_dividends(dividends, taxes)
    final_results = convert_result_fields(matched_results)

    if isin_name_map:
        name_missing_count = 0
        isin_missing_count = 0
        
        for r in final_results:
            isin = r.get("ISIN")
            if not isin:
                isin_missing_count += 1
                print(f"Warning: Entry for {r['name']} has no ISIN code")
                continue
                
            if isin in isin_name_map:
                r["name"] = isin_name_map[isin]
            else:
                name_missing_count += 1
                print(f"Warning: No company name found for ISIN {isin} (Ticker: {r['name']})")

        # Summary warnings
        if name_missing_count:
            print(f"\nⓘ  notice: Couldn't find names for {name_missing_count} ISIN(s)")
        if isin_missing_count:
            print(f"ⓘ  notice: {isin_missing_count} entries lack ISIN codes")

    else:  # No isin_name_map available
        if args['input_mode'] == "two":
            print("\n⚠️  Warning: Financial Instrument Information file provided but no valid names extracted")
        else:
            print("\nⓘ  notice: No company names available in input file")
        print("           Using tickers as fallback identifiers\n")    

    print(f"\nⓘ  notice: The tax year of the dividends appears to be {tax_year} (based on {len(kept_divs)}/{total_divs} dividends)\n")

    # After processing all dividends but before writing output:
    if duplicate_warning_count > 0:
        analyze_duplicate_dividends(duplicate_records)
        invalid_output = True

    too_many_ignored_divs = False
    if total_divs > 0:
        too_many_ignored_divs = (len(ignored_divs) / total_divs) > 0.3

    # --- Summary warnings ---
    if (orphan_tax_warning_count > 0 or
        unknown_isin_count > 0 or
        duplicate_warning_count > 0 or
        duplicate_tax_warning_count > 0 or
        positive_tax_warning_count > 0 or
        identical_tax_warning_count > 0 or
        unusual_tax_warning_count > 0 or
        very_unusual_tax_warning_count > 0 or
        unusually_low_tax_warning_count > 0 or
        ignored_divs):

        print("\n⚠️    Summary of the issues:")


        if unknown_isin_count > 0:
            print(f"\n🟠 {unknown_isin_count} WARNING(s) about unknown ISIN country codes.")

        if orphan_tax_warning_count > 0:
            print(f"\n🟠 {orphan_tax_warning_count} WARNING(s) about orphan withholding tax rows.")

        if duplicate_tax_warning_count > 0:
            print(f"\n🔴 {duplicate_tax_warning_count} WARNING(s) about duplicate withholding tax rows.")

        if positive_tax_warning_count > 0:
            print(f"\n🔴 {positive_tax_warning_count} WARNING(s) about positive withholding tax rows. Taxes should be always negative or zero, not positive.")

        if ignored_divs:
            print(f"\n🟠 {len(ignored_divs)} dividend(s) ignored from different tax year(s). We assume the tax year is {tax_year} (based on {len(kept_divs)}/{total_divs} dividends)")
            if too_many_ignored_divs:
                print("  More than 30% of dividends are outside the detected tax year — please verify your report period.")

        # Tax rate warnings (high priority)
        if unusually_low_tax_warning_count > 0:
            print(f"\n🟠 {unusually_low_tax_warning_count} WARNING(s) about LOW TAX RATES")
            print("  • Below expected minimum rates")
        
        if unusual_tax_warning_count > 0:
            print(f"\n🟠 {unusual_tax_warning_count} WARNING(s) about HIGH TAX RATES")
            print("  • Above expected treaty rates")
            print("  • Check for missing tax relief forms")
        
        if very_unusual_tax_warning_count > 0:
            print(f"\n🔴 {very_unusual_tax_warning_count} WARNING(s) about EXTREME TAX RATES")
            print("  • Over 50% withholding tax detected")
            print("  • Immediate verification required")

        if identical_tax_warning_count > 0:
            invalid_output = True
            print(f"\n🔴 {identical_tax_warning_count} CRITICAL WARNING(s) about identical tax adjustments")
            print("   This means your input file contains exact duplicate lines")
            print("   🚫 THE GENERATED OUTPUT IS INVALID AND SHOULD NOT BE USED FOR TAX PURPOSES")
            print("   Solution: Regenerate your IBKR report with proper settings to avoid duplicates")

        if duplicate_warning_count > 0:
            invalid_output = True
            print(f"\n🔴 {duplicate_warning_count} CRITICAL WARNING(s) about duplicate dividends")
            print("   Each duplicate will cause double-counting in tax calculations")
            print("   🚫 THE OUTPUT IS INVALID while duplicates exist")
            if duplicate_warning_count >= len(dividends) // 2:
                print("   Duplicate dividends are 50% or more of total.")
            print("   Solution: Check your IBKR report settings to avoid duplicate entries")

        if (orphan_tax_warning_count > 0 or
            unknown_isin_count > 0 or
            duplicate_warning_count > 0 or
            duplicate_tax_warning_count > 0 or
            positive_tax_warning_count > 0 or
            identical_tax_warning_count > 0 or
            very_unusual_tax_warning_count > 0 or
            too_many_ignored_divs):
            serious_issues = True
            print("\n⚠️  WARNING: Serious issues with the input data are encountered, check if the input data is correct or fix manually the errors in the output.")

    if duplicate_sections:
        serious_issues = True
        print("\n🚨  CRITICAL WARNING: Duplicate sections detected in input file:")
        for section in duplicate_sections:
            print(f"  - {section} section appears multiple times")
        print("  This typically happens when generating Custom Statements with all possible sections")
        print("  SOLUTION: Regenerate your report with only the needed sections (Dividends and Withholding Tax)\n")

    if invalid_output:
        serious_issues = True
        print("\n💀 WARNING: Due to duplicate data found in input file,")
        print("            THE OUTPUT IS NOT VALID FOR TAX REPORTING")
        print("            Use only for debugging purposes")
        print("            Regenerate a clean report from IBKR and try again")

    print("")
    write_output(final_results, output_file, mode)

    if serious_issues:
        exit(1)

if __name__ == "__main__":
    main()

