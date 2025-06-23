#!/usr/bin/env python3
# ibkr_ods_exporter.py

import os
import xml.etree.ElementTree as ET
import re
import argparse
import sys
import json
from collections import defaultdict, OrderedDict
from datetime import datetime
from dateutil import parser
from zoneinfo import ZoneInfo  # Python 3.9+
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import List, Dict, Tuple, Callable, Optional

# ODF specific imports
from odf.opendocument import OpenDocumentSpreadsheet
from odf.style import Style, TextProperties, TableCellProperties
from odf.table import Table, TableRow, TableCell, TableColumn
from odf.text import P

# LXML for more advanced XML parsing (if needed, otherwise ET is usually sufficient)
from lxml import etree 

# Local imports
from process_IBKR_dividends import look_for_currency_rate, round_decimal


# Set higher precision for Decimal
getcontext().prec = 28  # Set precision to 28 digits to avoid precision issues

ASSET_CATS = {
    "STK", "FUND", "CFD", "FXCFD", "OPT", "FUT",
    "FOP", "WAR", "CMDTY"
}

ASSET_CATS_IGNORE = {
    "CASH"
}


def parse_all_trades_from_dir(xml_dir):
    """
    Load all <Trade> and <Lot> elements from XML files in a given directory.
    Returns a list of ElementTree elements.
    """
    all_elements = []

    for fname in sorted(os.listdir(xml_dir)):
        if not fname.lower().endswith(".xml"):
            continue

        print(f"[debug] Processing XML file at parse_all_trades_from_dir(): {fname}")

        fpath = os.path.join(xml_dir, fname)
        try:
            tree = ET.parse(fpath)
            root = tree.getroot()
            trades = root.findall('.//Trades/*')
            all_elements.extend(trades)
        except ET.ParseError as e:
            print(f"[ERROR] Failed to parse XML file '{fname}': {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error processing file '{fname}': {e}")

    return all_elements

def index_opening_trades(elements):
    opens = {}
    for el in elements:
        if el.tag != "Trade":
            continue
        if el.get("levelOfDetail") != "EXECUTION":
            continue
        if el.get("openCloseIndicator") != "O":
            continue

        asset_category = el.get("assetCategory")

        if asset_category not in ASSET_CATS:
            if asset_category in ASSET_CATS_IGNORE:
                continue
            else:
                print(f"WARNING: Unexpected assetCategory \"{asset_category}\": ",  ET.tostring(el, encoding="unicode") )

        tid = el.get("transactionID")
        if not tid:
            print("WARNING: Trade element missing transactionID. Skipping. Context: ", ET.tostring(el, encoding="unicode"))
            continue

        # Extracting relevant attributes
        symbol = el.get("symbol")
        buy_sell = el.get("buySell")
        trade_date = el.get("tradeDate")
        date_time = el.get("dateTime")
        price = Decimal(el.get("tradePrice", "0"))  # Default to 0 if not found
        currency = el.get("currency")
        sub_category = el.get("subCategory")
        description = el.get("description")
        isin = el.get("isin")
        exchange = el.get("exchange")
        fx_rate = Decimal(el.get("fxRateToBase", "1") or "1")  # Default to 1 if not found

        # New attributes to include
        quantity = el.get("quantity", "0")  # Default to 0 if missing
        cost = Decimal(el.get("cost", "0"))  # Default to 0 if missing
        trade_money = Decimal(el.get("tradeMoney", "0"))  # Default to 0 if missing
        proceeds = Decimal(el.get("proceeds", "0"))  # Default to 0 if missing
        order_time = el.get("orderTime")
        net_cash = Decimal(el.get("netCash", "0"))  # Default to 0 if missing
        close_price = Decimal(el.get("closePrice", "0"))  # Default to 0 if missing
        fifo_pnl_realized = Decimal(el.get("fifoPnlRealized", "0"))  # Default to 0 if missing
        mtm_pnl = Decimal(el.get("mtmPnl", "0"))  # Default to 0 if missing
        ib_commission = Decimal(el.get("ibCommission", "0"))  # Default to 0 if missing
        settle_date_target = el.get("settleDateTarget")

        if not is_valid_timestamp(date_time):
            print(f"WARNING: CHECK YOUR FLEX QUERY DATE SETTINGS! The dateTime format is unexpected (in index_opening_trades)! date_time = \"{date_time}\"")


        try:
            # Add all collected data to the dictionary
            opens[tid] = {
                "symbol": symbol,
                "buySell": buy_sell,
                "tradeDate": trade_date,
                "dateTime": date_time,
                "price": price,
                "currency": currency,
                "assetCategory": asset_category,
                "subCategory": sub_category,
                "description": description,
                "isin": isin,
                "exchange": exchange,
                "fxRate": fx_rate,
                "quantity": Decimal(quantity),  # Ensuring it's a Decimal
                "cost": cost,
                "tradeMoney": trade_money,
                "proceeds": proceeds,
                "orderTime": order_time,
                "netCash": net_cash,
                "closePrice": close_price,
                "fifoPnlRealized": fifo_pnl_realized,
                "mtmPnl": mtm_pnl,
                "ibCommission": ib_commission,
                "settleDateTarget": settle_date_target
            }
        except Exception as e:
            print(f"[ERROR] Failed to process opening trade with transactionID={tid}: {e}")

    return opens

def process_closing_trades(elements, opens, convert_date=False):
    results = []
    i = 0
    while i < len(elements):
        el = elements[i]
        
        # Skip non-Trade elements or non-closing trades
        if el.tag != "Trade" or el.get("levelOfDetail") != "EXECUTION" or el.get("openCloseIndicator") != "C":
            i += 1
            continue

        # Validate asset category (NEW CODE)
        asset_category = el.get("assetCategory")
        if asset_category not in ASSET_CATS:
            if asset_category not in ASSET_CATS_IGNORE:
                print(f"WARNING: Unexpected assetCategory \"{asset_category}\" in closing trade: ", 
                      ET.tostring(el, encoding="unicode"))
            i += 1
            continue

        close_bs = el.get("buySell")
        symbol = el.get("symbol")
        close_date = el.get("tradeDate")
        date_close_fmt = format_date(close_date)
        close_dateTime = el.get("dateTime")

        if close_date == close_dateTime:
            print(f"WARNING: CHECK YOUR FLEX QUERY DATE SETTINGS! dateTime contains only a date (in process_closing_trades)! close_dateTime = \"{close_dateTime}\" close_date = \"{close_date}\"")

        if not is_valid_timestamp(close_dateTime):
            print(f"WARNING: CHECK YOUR FLEX QUERY DATE SETTINGS! The dateTime format is unexpected (in process_closing_trades)! close_dateTime = \"{close_dateTime}\"")

        if close_dateTime:
            if close_dateTime.split(";")[0] != close_date:
                print(f"WARNING: dateTime does not match (in process_closing_trades)! close_dateTime = \"{close_dateTime}\" close_date = \"{close_date}\"")

        close_price = Decimal(el.get("tradePrice"))
        close_qty = abs(Decimal(el.get("quantity")))
        close_currency = el.get("currency")
        fx_close = Decimal(el.get("fxRateToBase", "1"))
        pos_type = "LONG" if close_bs == "SELL" else "SHORT"

        print_descr = el.get("description")
        print(f"[debug] Found closing trade: {symbol} \"{print_descr}\" date_close_fmt={date_close_fmt} qty={close_qty}")

        lots = []
        j = i + 1
        while j < len(elements) and elements[j].tag == "Lot":
            lots.append(elements[j])
            j += 1

        if not lots:
            print(f"[ERROR]    No <Lot> found for closing trade {symbol} \"{print_descr}\" on {date_close_fmt}. Get another FlexQuery with enabled \"Closed Lots\" subsection at the \"Trades\" section and try again.")

        for lot in lots:
            lot_close_date=lot.get("tradeDate")
            if lot_close_date:
                if lot_close_date != close_date:
                    print("WARNING: tradeDate do not match (closing lot)!")
            else:
                print("WARNING: tradeDate is missing (closing lot).")

            # fallback
            close_dateTime_final = close_dateTime

            lot_close_dateTime = lot.get("dateTime")
            if lot_close_dateTime:
                if lot_close_dateTime != close_dateTime:
                    print("WARNING: dateTime do not match (closing lot)!")
                close_dateTime_final = lot_close_dateTime
            else:
                print("WARNING: dateTime is missing (closing lot).")

            date_close_fmt = format_date(close_dateTime_final)

            orig_close, sofia_close = convert_to_sofia_date(close_dateTime_final)
                            
            if not orig_close or not sofia_close:
                print(f"WARNING:   Close date timezone shift can't be computed. Context: {symbol} \"{print_descr}\" date_close_fmt={date_close_fmt} close_qty={close_qty}")
            if orig_close != sofia_close:
                print(f"WARNING:   Close date changes in Sofia timezone: {orig_close} → {sofia_close} Context: {symbol} \"{print_descr}\" date_close_fmt={date_close_fmt} close_qty={close_qty}")
                if convert_date:
                    print(f"           Calculations will be made with the date {sofia_close} (according to the Sofia time zone).")
                    date_close_fmt = sofia_close
            else:
                print(f"[debug]:      No CLOSE date changes because of time zones. Context: {symbol} \"{print_descr}\" date_close_fmt={date_close_fmt} close_qty={close_qty}")

            lot_qty = Decimal(lot.get("quantity"))
            open_tid = lot.get("transactionID")
            op = opens.get(open_tid)

            gross_close = (close_price * lot_qty).quantize(Decimal("0.01"))
            rate_close = look_for_currency_rate(close_currency, date_close_fmt)

            open_date_fmt = None

            currency_code = None

            if op:
                approx_open = False
                open_price = op["price"]
                open_currency = op["currency"]
                open_date = op["tradeDate"]
                open_dateTime = op["dateTime"] # precise date and time
                if open_dateTime:
                    if open_dateTime.split(";")[0] != open_date:
                        print("WARNING: dateTime do not match (open lot)!")
                currency_code = open_currency
                gross_open = (open_price * lot_qty).quantize(Decimal("0.01"))
            else:
                approx_open = True
                print("DEBUG: if not op")
                realized_pnl_base = Decimal(lot.get("fifoPnlRealized") or lot.get("realizedPnL") or "0")
                print("DEBUG: realized_pnl_base:", realized_pnl_base)

                fx_to_base = fx_close if fx_close != 0 else Decimal("1")
                print("DEBUG: fx_to_base:", fx_to_base)
                realized_pnl_local = (realized_pnl_base / fx_to_base).quantize(Decimal("0.01"))
                print("DEBUG: realized_pnl_local:", realized_pnl_local)

                if pos_type == "LONG":
                    gross_open = (gross_close - realized_pnl_local).quantize(Decimal("0.01"))
                else:
                    gross_open = (gross_close + realized_pnl_local).quantize(Decimal("0.01"))

                print("DEBUG: gross_open:", gross_open)
                open_price = None

                open_dateTime = lot.get("openDateTime")  # Example: '20240702;113930 EDT'
                if not open_dateTime:
                    print("ERROR: open_dateTime can't be determined!")

                currency_code = close_currency

            if not open_dateTime:
                print("ERROR: open_dateTime varialbe is not set!")

            open_date_fmt = format_date(open_dateTime) # simple formatting, without converting date
            
            orig_open, sofia_open = convert_to_sofia_date(open_dateTime)
            if not orig_open or not sofia_open:
                print(f"WARNING:   Open date timezone shift can't be computed. Context: {symbol} \"{print_descr}\" open_date_fmt: {open_date_fmt}")
            if orig_open != sofia_open:
                print(f"WARNING:   Open date changes in Sofia timezone: {orig_open} → {sofia_open} Context: {symbol} \"{print_descr}\" open_date_fmt: {open_date_fmt}")
                if convert_date:
                    print(f"           Calculations will be made with the date {sofia_open} (according to the Sofia time zone).")
                    open_date_fmt = sofia_open
            else:
                print(f"[debug]:      No OPEN date changes because of time zones. Context: {symbol} \"{print_descr}\" open_date_fmt: {open_date_fmt}")

            rate_open = look_for_currency_rate(currency_code, open_date_fmt)

            # Determine BuyCurrency and SellCurrency
            if pos_type == "LONG":
                buy_currency = op["currency"] if op else close_currency
                sell_currency = close_currency
                buy_rate = rate_open
                sell_rate = rate_close
            else:
                buy_currency = close_currency
                sell_currency = op["currency"] if op else close_currency
                buy_rate = rate_close
                sell_rate = rate_open

            # Compute BGN values (precisely first, then round to 0.01)
            if pos_type == "LONG":
                buy_bgn = gross_open * buy_rate
                sell_bgn = gross_close * sell_rate
            else:
                sell_bgn = gross_open * sell_rate
                buy_bgn = gross_close * buy_rate

            # Round the computed gross values to 0.01
            buy_bgn = buy_bgn.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            sell_bgn = sell_bgn.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            profit_bgn = None
            loss_bgn = None
            pl = (sell_bgn - buy_bgn).quantize(Decimal("0.01"))
            if pl >= 0:
                profit_bgn = pl
            else:
                loss_bgn = -pl

            description = el.get("description") if pos_type == "LONG" else op.get("description", "") if op else ""
            
            print(f"[debug]    Adding trade_data about closing trade: {symbol} \"{description}\" date_close_fmt={date_close_fmt} close_qty={close_qty} lot_qty={lot_qty}")
          
            trade_data = {
                "PositionType": pos_type,
                "Approximation": "Yes" if approx_open else "",
                "OpenDate": open_date_fmt,
                "CloseDate": date_close_fmt,
                "BuyCurrency": buy_currency,
                "SellCurrency": sell_currency,
                "BuyCurrencyRate": str(buy_rate),
                "SellCurrencyRate": str(sell_rate),
                "Quantity": f"{lot_qty}",
                "OpenPricePerShare": f"{open_price}" if open_price else "",
                "ClosePricePerShare": f"{close_price}",
                "OpenGrossTotal": f"{gross_open}",
                "CloseGrossTotal": f"{gross_close}",
                "BuyGrossBGN": f"{buy_bgn}",
                "SellGrossBGN": f"{sell_bgn}",
                "ProfitBGN": f"{profit_bgn}" if profit_bgn else "",
                "LossBGN": f"{loss_bgn}" if loss_bgn else "",
                "assetCategory": el.get("assetCategory") if pos_type == "LONG" else op.get("assetCategory", "") if op else "",
                "subCategory": el.get("subCategory") if pos_type == "LONG" else op.get("subCategory", "") if op else "",
                "symbol": el.get("symbol") if pos_type == "LONG" else op.get("symbol", "") if op else "",
                "description": description,
                "isin": el.get("isin") if pos_type == "LONG" else op.get("isin", "") if op else "",
                "exchange": el.get("exchange") if pos_type == "LONG" else op.get("exchange", "") if op else ""
            }

            results.append(trade_data)
        i = j
    return results
  
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")
   

def collect_open_positions(flex_files, opens, convert_date=False):

    open_positions = []

    trades_by_txn = {}
    for file in flex_files:
        if file.stat().st_size == 0:
            print(f"[warning] Skipping empty file: {file}")
            continue

        try:
            tree = etree.parse(str(file))
        except etree.XMLSyntaxError as e:
            print(f"[warning] Skipping malformed XML file: {file} - {e}")
            continue

        print(f"[debug] Processing XML file at collect_open_positions(): {file}")

        to_date_match = re.search(r'toDate="(\d{8})"', open(file, 'r').read())
        to_date = to_date_match.group(1) if to_date_match else ""
        to_date_formatted = format_date(to_date) if to_date else ""

        # Collect all trades into map by originatingTransactionID
        for t in tree.xpath("//Trade"):
            txn_id = t.get("transactionID") # not originatingTransactionID!
            if txn_id:
                trades_by_txn[txn_id] = t.attrib

        for pos in tree.xpath("//OpenPosition[@side='Long'][@levelOfDetail='LOT'][@assetCategory='STK' or @assetCategory='FUND']"):
            isin = pos.get("isin")
            symbol = pos.get("symbol")
            position = Decimal(pos.get("position"))
            country = pos.get("issuerCountryCode")
            open_datetime = pos.get("openDateTime")
            originating_txn_id = pos.get("originatingTransactionID")
            currency = pos.get("currency")
            description = pos.get("description", "")

            if not is_valid_timestamp(open_datetime):
                print(f"WARNING: CHECK YOUR FLEX QUERY DATE SETTINGS! openDateTime format is unexpected (in collect_open_positions)! open_datetime = \"{open_datetime}\"")

            approx_flag = ""
            price_in_currency = ""
            date_formatted = ""
            currency_output = ""
            price_bgn = ""

            print(f"[debug] Looking for trade {originating_txn_id}...")
            trade = trades_by_txn.get(originating_txn_id)

            if trade:
                qty_str = trade.get("quantity")
                if qty_str is None:
                    print(f"[ERROR] Missing 'quantity' attribute in OpenPosition: {json.dumps(trade, indent=2, default=decimal_default)}")
                    trade_qty = Decimal(0)
                else:
                    trade_qty = Decimal(qty_str)

                trade_price = Decimal(trade.get("tradePrice"))
                trade_money = Decimal(trade.get("tradeMoney"))
                trade_currency = trade.get("currency")
                trade_datetime = trade.get("dateTime")
                trade_date = trade.get("tradeDate")
                trade_isin = trade.get("isin")

                gross_calc = trade_price * position
                price_in_currency = gross_calc.quantize(Decimal("0.01"))

                if abs(gross_calc - price_in_currency) > Decimal(0):
                    print(f"[warning] Mismatch after rounding calculated gross value for {symbol} (ISIN {isin}): price_in_currency={price_in_currency}, gross_calc={gross_calc}, tradeMoney={trade_money}")

                if abs(gross_calc - trade_money) > Decimal(0):
                    print(f"[warning] (expected if partial sale) Mismatch in calculated gross value vs tradeMoney for {symbol} (ISIN {isin}): gross_calc={gross_calc}, tradeMoney={trade_money}")

                if abs(position - trade_qty) > Decimal(0):
                    print(f"[warning] (expected if partial sale) Mismatch in quantity for {symbol} (ISIN {isin}): OpenPosition={position}, Trade={trade_qty}")

                if currency != trade_currency:
                    print(f"[warning] Currency mismatch for {symbol} (ISIN {isin}): OpenPosition={currency}, Trade={trade_currency}")

                if open_datetime != trade_datetime:
                    print(f"[warning] DateTime mismatch for {symbol} (ISIN {isin}): OpenPosition={open_datetime}, Trade={trade_datetime}")

                if isin != trade_isin:
                    print(f"[warning] ISIN mismatch for {symbol}: OpenPosition={isin}, Trade={trade_isin}")

                if trade_datetime:
                    date_output = trade_datetime
                else:
                    date_output = trade_date
                currency_output = trade_currency
            else:
                # Fallback to costBasisMoney
                print(f"[warning] No trade match for originatingTransactionID {originating_txn_id} — using fallback data for {symbol} \"{description}\" (ISIN {isin})")
                approx_flag = "Yes"
                price_in_currency = Decimal(pos.get("costBasisMoney")).quantize(Decimal("0.01"))
                
                date_output = open_datetime
                currency_output = currency

            date_formatted = format_date(date_output) # simple formatting, without converting date

            orig_date_output, sofia_date_output = convert_to_sofia_date(date_output)

            if not orig_date_output or not sofia_date_output:
                print(f"WARNING:   Open date (for Open Positions sheet) timezone shift can't be computed. Context: {symbol} \"{description}\" open_datetime={open_datetime} position={position}")
                print(f"           orig_date_output={orig_date_output} sofia_date_output={sofia_date_output} date_output={date_output}")
            if orig_date_output != sofia_date_output:
                print(f"WARNING:   Open date (for Open Positions sheet) changes in Sofia timezone: {orig_date_output} → {sofia_date_output} Context: {symbol} \"{description}\" open_datetime={open_datetime} position={position}")
                if convert_date:
                    print(f"           Calculations will be made with the date {sofia_date_output} (according to the Sofia time zone).")
                    date_formatted = sofia_date_output
            else:
                print(f"[debug]:      No OPEN date (for Open Positions sheet) changes because of time zones. Context: {symbol} \"{description}\" open_datetime={open_datetime} position={position}")

            currency_rate_output = Decimal(look_for_currency_rate(currency_output, date_formatted))
            price_bgn = (currency_rate_output * Decimal(price_in_currency)).quantize(Decimal("0.01"))

            open_positions.append({
                "toDate": to_date_formatted,
                "Approximation": approx_flag,
                "currency": currency_output,
                "currency rate": currency_rate_output,
                "country": country,
                "count": position,
                "date": date_formatted,
                "price_in_currency": price_in_currency,
                "price": price_bgn,
                "assetCategory": pos.get("assetCategory", ""),
                "subCategory": pos.get("subCategory", ""),
                "symbol": symbol,
                "description": description,
                "isin": isin
            })

    return open_positions

def process_dividends_from_xml(xml_dir: str, convert_date: bool = False) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Processes IBKR Flex Query XML files to extract and consolidate dividend
    and withholding tax information by actionID, handling corrections.
    It prepares data for three different ODS sheet formats.

    Args:
        xml_dir (str): Path to the directory containing XML files.
        convert_date (bool): If True, dates will be converted to Sofia time zone.

    Returns:
        Tuple[List[Dict], List[Dict], List[Dict]]: Three lists of dictionaries
        for 'dividends-nap-autopilot', 'dividends-sheet', and 'dividends-table' sheets, respectively.
    """
    securities_info_map: Dict[str, Dict] = {} # ISIN -> {'name': str, 'country': str}
    transactions_by_action_id = defaultdict(lambda: {'dividends': [], 'taxes': [], 'raw_details': []})

    all_xml_files = sorted([p for p in Path(xml_dir).iterdir() if p.suffix.lower() == '.xml'])

    if not all_xml_files:
        print(f"No XML files found in directory: {xml_dir}")
        return [], [], []

    print("\n[debug] Processing XML files for dividends and taxes...")

    for fpath in all_xml_files:
        print(f"[debug] Parsing {fpath.name} for dividend data...")
        try:
            tree = ET.parse(fpath)
            root = tree.getroot()

            # First pass: Collect SecuritiesInfo for name and country mapping
            for sec_info in root.iter('SecurityInfo'):
                isin = sec_info.get('isin')
                description = sec_info.get('description')
                issuer_country_code_sec = sec_info.get('issuerCountryCode') # Renamed to avoid conflict
                if isin and description and issuer_country_code_sec:
                    securities_info_map[isin] = {'name': description, 'country': issuer_country_code_sec}

            # Second pass: Collect CashTransaction details
            for transaction in root.iter('CashTransaction'):
                if transaction.get('levelOfDetail') == 'DETAIL':
                    action_id = transaction.get('actionID')
                    transaction_type = transaction.get('type')
                    amount_str = transaction.get('amount')

                    if not action_id or not amount_str:
                        continue # Skip entries without critical data

                    try:
                        amount = Decimal(amount_str)
                    except InvalidOperation:
                        print(f"Warning: Could not parse amount '{amount_str}' for actionID {action_id}. Skipping.")
                        continue

                    # Store the raw attributes for later common data extraction
                    transactions_by_action_id[action_id]['raw_details'].append(transaction.attrib)

                    if transaction_type == 'Dividends' or transaction_type == 'Payment In Lieu Of Dividends':
                        transactions_by_action_id[action_id]['dividends'].append(amount)
                    elif transaction_type == 'Withholding Tax':
                        transactions_by_action_id[action_id]['taxes'].append(amount)

        except ET.ParseError as e:
            print(f"Error parsing XML file {fpath.name}: {e}")
            continue
        except Exception as e:
            print(f"An unexpected error occurred while processing {fpath.name}: {e}")
            continue

    # Prepare data for different sheet formats
    dividends_nap_autopilot_data = []
    dividends_sheet_data = []
    dividends_table_data = []

    for action_id, data in transactions_by_action_id.items():
        total_dividend_amount = sum(data['dividends'])
        total_tax_amount = sum(data['taxes'])

        # Skip if no actual dividend (e.g., only tax correction entries)
        # or if there are only tax entries that net to zero (pure corrections without a dividend)
        if not data['dividends'] and not data['taxes']:
            continue

        # Find a representative detail record for common information (prefer dividend if available)
        representative_detail = None
        for detail in data['raw_details']:
            if detail.get('type') == 'Dividends' or detail.get('type') == 'Payment In Lieu Of Dividends':
                representative_detail = detail
                break
        if not representative_detail and data['raw_details']: # Fallback to first available detail
            representative_detail = data['raw_details'][0]

        if not representative_detail:
            print(f"Warning: No valid detail record found for actionID {action_id}. Skipping.")
            continue



        # Extract common info
        isin = representative_detail.get('isin', '')
        symbol = representative_detail.get('symbol', '')
        currency = representative_detail.get('currency', '')
        date_time_raw = representative_detail.get('dateTime', '')
        issuer_country_code = representative_detail.get('issuerCountryCode', '') # Use this for initial check

        # Determine name and country
        name = securities_info_map.get(isin, {}).get('name', symbol)
        country = issuer_country_code if issuer_country_code else securities_info_map.get(isin, {}).get('country', '')


        # --- Date Consistency Check ---
        all_relevant_transaction_dates = set()

        # Iterate through raw_details to collect dates from both dividend and tax types
        for detail in data['raw_details']:
            transaction_type = detail.get('type')
            date_time = detail.get('dateTime')

            if date_time:
                # Only consider relevant types for the date consistency check
                if transaction_type in ['Dividends', 'Payment In Lieu Of Dividends', 'Withholding Tax']:
                    all_relevant_transaction_dates.add(date_time)

        # A mismatch occurs if there's more than one unique date among all relevant transactions
        if len(all_relevant_transaction_dates) > 1:
            print(f"WARNING: Date mismatch for actionID {action_id}. "
                  f"Found multiple unique dates: {', '.join(sorted(list(all_relevant_transaction_dates)))}. "
                  f"Context: {symbol} \"{name}\"")
        # --- END Date Consistency Check ---

        dividend_date = format_date(date_time_raw) # simple formatting, without converting date
        
        orig_dividend_date, sofia_dividend_date = convert_to_sofia_date(date_time_raw)

        if not orig_dividend_date or not sofia_dividend_date:
            print(f"WARNING:   Dividend date timezone shift can't be computed. Context: {symbol} \"{name}\" date_time_raw: {date_time_raw}")
        if orig_dividend_date != sofia_dividend_date:
            print(f"WARNING:   Dividend date changes in Sofia timezone: {orig_dividend_date} → {sofia_dividend_date} Context: {symbol} \"{name}\" date_time_raw: {date_time_raw}")
            if convert_date:
                print(f"           Calculations will be made with the date {sofia_dividend_date} (according to the Sofia time zone).")
                dividend_date = sofia_dividend_date
            else:
                dividend_date = orig_dividend_date

        else:
            print(f"[debug]:      No dividend date changes because of time zones. Context: {symbol} \"{name}\" date_time_raw: {date_time_raw}")
        
        # Pass the DD.MM.YYYY formatted date to look_for_currency_rate
        bgn_rate = Decimal(look_for_currency_rate(currency, dividend_date)) if currency != 'BGN' and dividend_date else Decimal('1.0')

        print(f"[debug]: Currency rate for {currency} at {dividend_date} is {bgn_rate}. Context: {symbol} \"{name}\" date_time_raw: {date_time_raw}")

        if total_tax_amount > 0:
            print(f"ERROR: positive withholding tax encountered!  Context: {symbol} \"{name}\" date_time_raw: {date_time_raw}, withheld tax: {total_tax_amount}")

        total_tax_amount = abs(total_tax_amount) # the tax is negative in Flex Query data, but positive in the output data

        dividend_bgn = (total_dividend_amount * bgn_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        withholding_tax_bgn = (total_tax_amount * bgn_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        income_code = "8141" # hardcoded

        permitted_tax_credit = round_decimal(dividend_bgn * Decimal("0.05"))

        if withholding_tax_bgn == Decimal("0"):
            tax_due = permitted_tax_credit
        else:
            tax_due = max(Decimal("0"), permitted_tax_credit - withholding_tax_bgn)

        applied_tax_credit = min(withholding_tax_bgn, permitted_tax_credit)

        if withholding_tax_bgn == Decimal("0"):
            method = "3"
            permitted_tax_credit = 0 # for consistency with nap-autopilot
        else:
            method = "1"

        # Data for dividends-nap-autopilot
        dividends_nap_autopilot_data.append({
            "name": name,
            "country": country,
            "sum": dividend_bgn,
            "paidtax": withholding_tax_bgn
        })

        # Data for dividends-sheet
        dividends_sheet_data.append({
            "name": name,
            "ISIN": isin,
            "currency code": currency,
            "dividend": total_dividend_amount,
            "withholding tax": total_tax_amount,
            "date": dividend_date,
            "currency rate": bgn_rate,
            "dividend BGN": dividend_bgn,
            "withholding tax BGN": withholding_tax_bgn,
            "permitted tax credit": permitted_tax_credit,
            "method": method,
            "applied tax credit": applied_tax_credit,
            "tax due": tax_due,
            "country": country
        })

        # Data for dividends-table
        dividends_table_data.append({
            "name": name,
            "country": country,
            "income code": income_code,
            "method": method,
            "dividend BGN": dividend_bgn,
            "withholding tax BGN": withholding_tax_bgn,
            "permitted tax credit": permitted_tax_credit,
            "applied tax credit": applied_tax_credit,
            "tax due": tax_due
        })

    print(f"[debug] Finished processing dividend data. Found {len(dividends_sheet_data)} consolidated dividend events.")
    return dividends_nap_autopilot_data, dividends_sheet_data, dividends_table_data

def process_interest_from_xml(xml_dir: str) -> List[Dict]:
    """
    Processes IBKR Flex Query XML files to extract interest and withholding tax info.
    Returns data for the 'Interest' sheet with proper separation between SYEP and cash interest.
    """
    interest_data = []
    all_xml_files = sorted([p for p in Path(xml_dir).iterdir() if p.suffix.lower() == '.xml'])

    # Separate storage for different interest types
    cash_interest_groups = defaultdict(lambda: {'interest': None, 'taxes': []})
    syep_interest_records = []

    account_ids = set()  # Track unique account IDs

    for fpath in all_xml_files:
        try:
            tree = ET.parse(fpath)
            root = tree.getroot()

            for tx in root.iter('CashTransaction'):
                if tx.get('levelOfDetail') != 'DETAIL':
                    continue

                tx_type = tx.get('type')
                
                interest_transaction_types = ["Broker Interest Received", "Withholding Tax"]

                if tx_type not in interest_transaction_types:
                    continue
               
                desc = tx.get('description', '').upper()  # Normalize case

                if "DIVIDEND" in desc:
                    continue

                # Only collect accountId from transactions we're keeping
                if account_id := tx.get('accountId'):
                    if account_id != "-":  # Skip SUMMARY records
                        account_ids.add(account_id)

                currency = tx.get('currency', '')
                date_time = tx.get('dateTime', '')
                amount = Decimal(tx.get('amount', '0'))
                
                # --- Date Processing ---
                date_formatted = ''
                if date_time:
                    try:
                        date_formatted = format_date(date_time)
                        if ';' in date_time or ' ' in date_time:
                            print(f"WARNING: Time component in dateTime for {tx_type}: {desc}")
                    except Exception as e:
                        print(f"WARNING: Failed to format date '{date_time}' for {desc}: {str(e)}")

                # --- Process SYEP Interest ---
                if "SYEP" in desc and tx_type == "Broker Interest Received":
                    syep_record = {
                        'description': desc,
                        'date': date_formatted,
                        'amount': amount,
                        'currency': currency,
                        'currency rate': '',
                        'amount BGN': '',
                        'withholding tax': '',
                        'withholding tax BGN': '',
                        'withholding tax date mismatch': ''
                    }
                    
                    # Attempt currency conversion for SYEP
                    if currency and date_formatted:
                        try:
                            bgn_rate = Decimal(look_for_currency_rate(currency, date_formatted))
                            syep_record['currency rate'] = bgn_rate
                            syep_record['amount BGN'] = (amount * bgn_rate).quantize(Decimal('0.01'))
                        except Exception as e:
                            print(f"WARNING: SYEP currency conversion failed for {currency}: {str(e)}")
                    
                    syep_interest_records.append(syep_record)
                    continue

                # --- Process Cash Interest and Taxes ---
                month_year = extract_month_year(desc)
                if not month_year:
                    continue

                key = (currency, month_year)

                if tx_type == "Broker Interest Received" and "CREDIT INT" in desc:
                    cash_interest_groups[key]['interest'] = {
                        'description': desc,
                        'date': date_formatted,
                        'amount': amount,
                        'currency': currency,
                        'raw_date': date_time
                    }
                elif tx_type == "Withholding Tax" and "ON CREDIT INT" in desc:
                    cash_interest_groups[key]['taxes'].append({
                        'amount': amount,
                        'date': date_formatted,
                        'raw_date': date_time,
                        'description': desc,
                        'currency': currency
                    })

        except Exception as e:
            print(f"Error processing {fpath.name}: {e}")

    # --- Process Cash Interest Groups ---
    for (currency, month_year), group in cash_interest_groups.items():
        interest = group['interest']
        taxes = group['taxes']

        if not interest:
            if taxes:
                print(f"WARNING: Orphaned withholding taxes for {currency} {month_year}")
            continue

        # --- Currency Conversion ---
        bgn_rate = ''
        amount_bgn = ''
        if currency and interest['date']:
            try:
                bgn_rate = Decimal(look_for_currency_rate(currency, interest['date']))
                amount_bgn = (interest['amount'] * bgn_rate).quantize(Decimal('0.01'))
            except Exception as e:
                print(f"WARNING: Currency conversion failed for {currency}: {str(e)}")

        print(f"debug: === Processing {currency} {month_year} - {len(taxes)} tax record(s) ===")

        # --- Tax Processing ---
        total_tax = Decimal('0')
        total_tax_bgn = Decimal('0')
        date_mismatch = False
        
        for tax in taxes:
            total_tax += tax['amount']
            if tax['raw_date'] != interest['raw_date']:
                date_mismatch = True

        if total_tax > 0:
            print(f"WARNING: Positive tax of {total_tax} for {currency} {month_year}")

        total_tax = abs(total_tax)

        tax_conversion_failed = False

        if date_mismatch:
            print("debug: calculating tax when date_mismatch is True... ")

            for tax in taxes:
                try:
                    tax_date = tax['date']
                    tax_currency_rate = Decimal(look_for_currency_rate(currency, tax_date))
                    tax_amount = tax['amount']
                    tax_piece_bgn = (tax_amount * tax_currency_rate).quantize(Decimal('0.01'))
                    tax_desc=tax.get('description', '')
                    print(f"debug: {tax_amount} {currency} -> {tax_piece_bgn} BGN / currency rate: {tax_currency_rate} date: {tax_date} / {tax_desc}")
                    total_tax_bgn += tax_piece_bgn
                except Exception as e:
                    print(f"WARNING: Tax conversion failed for tax on {tax['date']}: {str(e)}")
                    tax_conversion_failed = True

            if tax_conversion_failed:
                total_tax_bgn = "COMPUTATION FAILED"
            else:
                if total_tax_bgn > 0:
                    print(f"WARNING: Positive tax in BGN (total_tax_bgn) of {total_tax_bgn} for {currency} {month_year}")
                total_tax_bgn = abs(total_tax_bgn)


            print(f"debug: Net tax for {currency} {month_year}: {total_tax} {currency} (BGN: {total_tax_bgn})")

        else:
            try:
                total_tax_bgn = (total_tax * bgn_rate).quantize(Decimal('0.01'))
            except:
                print(f"WARNING: Tax conversion failed for {currency} {month_year}")

        interest_data.append({
            'description': interest['description'],
            'date': interest['date'],
            'amount': interest['amount'],
            'currency': currency,
            'currency rate': bgn_rate,
            'amount BGN': amount_bgn,
            'withholding tax': total_tax if taxes else '',
            'withholding tax BGN': total_tax_bgn if taxes else '',
            'withholding tax date mismatch': 'yes' if date_mismatch else ''
        })

    # --- Add SYEP Records ---
    interest_data.extend(syep_interest_records)

    if not account_ids:
        print("WARNING: No valid account IDs found in DETAIL records for interest transactions")
    elif len(account_ids) > 1:
        print(f"CRITICAL: Found interest transactions from multiple accounts: {account_ids}")
    elif len(account_ids) == 1:
        print(f"debug: All interest transactions from single account: {account_ids.pop() if account_ids else 'None'}")

    return interest_data

def extract_month_year(desc: str) -> Optional[str]:
    """Extracts month/year from descriptions like 'FOR MAR-2024'"""
    try:
        if "FOR " in desc:
            return desc.split("FOR ")[-1].strip().split()[0]  # Handle cases with extra text
    except:
        pass
    return None


def format_date(date_str, out_fmt="%d.%m.%Y"):
    """Convert 'YYYYMMDD' or 'YYYY-MM-DD' to 'DD.MM.YYYY' (or another desired format)."""
    date_str=date_str.split(";")[0]
    if "-" in date_str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        dt = datetime.strptime(date_str, "%Y%m%d")
    return dt.strftime(out_fmt)

def convert_to_sofia_date(datetime_str):
    """
    Convert dateTime like '20240703;055739 EDT' to Sofia time.
    Returns (original_date, sofia_date) in format DD.MM.YYYY.
    """
    tzinfos = {
        "EST": -18000,  # UTC-5
        "EDT": -14400,  # UTC-4
        "CST": -21600,  # UTC-6
        "CDT": -18000,  # UTC-5
        "MST": -25200,  # UTC-7
        "MDT": -21600,  # UTC-6
        "PST": -28800,  # UTC-8
        "PDT": -25200,  # UTC-7
        "GMT": 0,
        "UTC": 0,
    }

    try:
        if not datetime_str or ";" not in datetime_str:
            return None, None

        date_part, time_with_tz = datetime_str.split(";")
        parts = time_with_tz.strip().split()

        # Extract timezone abbreviation if present
        if len(parts) == 2:
            time_part, tz_abbr = parts
            if tz_abbr not in tzinfos:
                print(f"WARNING: Timezone abbreviation '{tz_abbr}' not recognized in tzinfos. May cause incorrect conversion.")
        else:
            time_part = parts[0]
            tz_abbr = ""

        dt_str = f"{date_part} {time_with_tz.strip()}"
        dt = parser.parse(dt_str, tzinfos=tzinfos)
        dt_sofia = dt.astimezone(ZoneInfo("Europe/Sofia"))

        fmt = "%d.%m.%Y"
        return dt.strftime(fmt), dt_sofia.strftime(fmt)

    except Exception as e:
        print(f"Error converting '{datetime_str}': {e}")
        return None, None

def is_valid_timestamp(date_str):
    """
    Validate if the date string matches the format 'YYYYMMDD;HHMMSS TZ' where:
    - YYYYMMDD is a valid date
    - HHMMSS is a valid time
    - TZ is a 3-letter timezone code
    
    Returns:
        bool: True if format is valid, False otherwise
    """
    if not date_str:
        return False
    
    pattern = r'^(\d{4})(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]);([01][0-9]|2[0-3])([0-5][0-9]){2} [A-Z]{3}$'
    
    if not re.fullmatch(pattern, date_str):
        return False
    
    # Additional date validation (check for invalid dates like 20230230)
    try:
        from datetime import datetime
        date_part = date_str.split(';')[0]
        datetime.strptime(date_part, '%Y%m%d')
        return True
    except ValueError:
        return False


def write_ods_with_totals(output_dir, sheets, output_file_name):
    """
    Write data to ODS file with totals sheet
    Args:
        output_dir: Directory to save the file
        sheets: List of sheet data
        output_file_name: Base filename (without .ods extension)
    """
    # Create path WITHOUT adding .ods extension
    output_path = Path(output_dir) / output_file_name

    doc = OpenDocumentSpreadsheet()

    # Define bold cell style
    bold_cell_style = Style(name="BoldCellStyle", family="table-cell")
    bold_cell_style.addElement(TextProperties(fontweight="bold"))
    doc.automaticstyles.addElement(bold_cell_style)

    for sheet_def in sheets:
        title = sheet_def["title"]
        rows = sheet_def["rows"]
        headers = sheet_def.get("headers")

        if not headers:
            # Generate headers from row keys if not provided, maintaining order
            headers = list(OrderedDict.fromkeys(k for row in rows for k in row.keys()))

        sheet = Table(name=title)
        # Add columns based on the number of headers
        for _ in headers:
            sheet.addElement(TableColumn())

        # Header row with bold styling
        header_row = TableRow()
        for head in headers:
            cell = TableCell(stylename=bold_cell_style)
            cell.addElement(P(text=head))
            header_row.addElement(cell)
        sheet.addElement(header_row)

        # Data rows
        for row in rows:
            trow = TableRow()
            for h in headers:
                val = row.get(h, "")
                cell = TableCell()
                try:
                    # Attempt to convert to Decimal for float valuetype
                    val_d = Decimal(str(val))
                    cell.setAttribute("valuetype", "float")
                    cell.setAttribute("value", str(val_d))
                    cell.addElement(P(text=str(val_d)))
                except:
                    # Handle formulas or other non-numeric values
                    if isinstance(val, str) and val.startswith("="):
                        # ODS formulas use semicolon as separator, replace comma
                        val = val.replace(",", ";")
                        cell.setAttribute("valuetype", "float") # Formulas are usually numeric results
                        cell.setAttribute("formula", val)
                    cell.addElement(P(text=str(val)))
                trow.addElement(cell)
            sheet.addElement(trow)

        doc.spreadsheet.addElement(sheet)

    # --- Totals Sheet ---
    totals_sheet = Table(name="Totals")
    # Add two columns for "Label" and "Value/Formula"
    totals_sheet.addElement(TableColumn())
    totals_sheet.addElement(TableColumn())

    # Only based on "Realized Trades"
    realized_sheet = next((s for s in sheets if s["title"] == "Realized Trades"), None)
    if realized_sheet:
        headers = realized_sheet["headers"]
        rows = realized_sheet["rows"] # Data rows, not including header

        total_fields = ["BuyGrossBGN", "SellGrossBGN", "ProfitBGN", "LossBGN"]

        for field in total_fields:
            if field not in headers:
                continue

            row = TableRow()
            # Label cell
            cell = TableCell()
            cell.addElement(P(text=f"Total {field}"))
            row.addElement(cell)

            # Formula cell
            # Column index starts from 0, so convert to A, B, C...
            col_letter = chr(65 + headers.index(field))
            # Formulas are 1-based for rows, and header is row 1, so data starts from row 2
            formula = f"=SUM('Realized Trades'.{col_letter}2:{col_letter}{len(rows)+1})"
            cell = TableCell(valuetype="float", formula=formula)
            row.addElement(cell)
            totals_sheet.addElement(row)

        # Total Net Profit (BGN)
        if "ProfitBGN" in headers and "LossBGN" in headers:
            row = TableRow()
            # Label cell
            cell = TableCell()
            cell.addElement(P(text="Total Net Profit (BGN)"))
            row.addElement(cell)

            # Formula cell
            p_col = chr(65 + headers.index("ProfitBGN"))
            l_col = chr(65 + headers.index("LossBGN"))
            formula = f"=SUM('Realized Trades'.{p_col}2:{p_col}{len(rows)+1}) - SUM('Realized Trades'.{l_col}2:{l_col}{len(rows)+1})"
            cell = TableCell(valuetype="float", formula=formula)
            row.addElement(cell)
            totals_sheet.addElement(row)

    # --- Add Total Interest BGN ---
    # Find the "Interest" sheet
    interest_sheet = next((s for s in sheets if s["title"] == "Interest"), None)
    if interest_sheet:
        interest_headers = interest_sheet["headers"]
        interest_rows = interest_sheet["rows"]

        # Check if "amount BGN" column exists in the Interest sheet
        if "amount BGN" in interest_headers:
            # Add a blank row for spacing, as requested ("one line below other totals")
            totals_sheet.addElement(TableRow())

            interest_total_row = TableRow()
            # Label cell
            cell_label = TableCell()
            cell_label.addElement(P(text="Total Interest (BGN)"))
            interest_total_row.addElement(cell_label)

            # Formula cell for "amount BGN"
            interest_col_index = interest_headers.index("amount BGN")
            interest_col_letter = chr(65 + interest_col_index)
            # Assuming data starts from row 2 (after header)
            interest_formula = f"=SUM('Interest'.{interest_col_letter}2:{interest_col_letter}{len(interest_rows)+1})"
            cell_formula = TableCell(valuetype="float", formula=interest_formula)
            interest_total_row.addElement(cell_formula)
            totals_sheet.addElement(interest_total_row)
        else:
            print("Warning: 'amount BGN' column not found in 'Interest' sheet.")
    else:
        print("Warning: 'Interest' sheet not found.")

    doc.spreadsheet.addElement(totals_sheet)

    # Save the document
    doc.save(str(output_path), True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("xml_dir", help="Path to directory containing XML files")
    parser.add_argument("--convert-date", action="store_true",
                        help="Convert dates to Sofia timezone (DD.MM.YYYY HH:MM:SS format)")
    args = parser.parse_args()

    xml_dir = args.xml_dir
    output_file_name = "ibkr_output" # without the .ods extension

    output_path = Path(xml_dir) / f"{output_file_name}.ods"

    # Check if file exists first
    if output_path.exists():
        print(f"\nERROR: Output file already exists at:\n{output_path}\n"
              "Please remove it or rename the existing file before running this exporter.")
        sys.exit(1)

    convert_date = args.convert_date

    # Process existing trades (assuming these functions are defined elsewhere in your file)
    # You should have these functions already from your original ibkr_ods_exporter.py
    try:
        elements = parse_all_trades_from_dir(xml_dir)
        opens = index_opening_trades(elements)
        # Pass the callable to existing trade functions
        results = process_closing_trades(elements, opens, convert_date)
        xml_files = [p for p in Path(xml_dir).iterdir() if p.suffix.lower() == '.xml']
        open_positions = collect_open_positions(xml_files, opens, convert_date)
    except Exception as e:
        print(f"Error processing trades or open positions: {e}")
        # Initialize to empty lists/dicts to proceed gracefully if an error occurs in trade processing
        elements, opens, results, open_positions = [], {}, [], []


    # Process dividends and taxes, passing the boolean flag directly
    dividends_nap, dividends_sheet, dividends_table = process_dividends_from_xml(xml_dir, convert_date)

    # Process interest data
    interest_data = process_interest_from_xml(xml_dir)

    # Define headers for the new sheets
    headers_nap_autopilot = ["name", "country", "sum", "paidtax"]

    headers_dividends_sheet = ["name", "ISIN", "currency code", "dividend", "withholding tax", "date",
                               "currency rate", "dividend BGN", "withholding tax BGN", "permitted tax credit",
                               "method", "applied tax credit", "tax due", "country"]

    headers_dividends_table = ["name", "country", "income code", "method", "dividend BGN",
                               "withholding tax BGN", "permitted tax credit", "applied tax credit", "tax due"]

    headers_interest = ["description", "date", "amount", "currency", "currency rate", "amount BGN",
                        "withholding tax", "withholding tax BGN", "withholding tax date mismatch"]

    # Initialize sheets list with existing sheets (if they exist)
    sheets = []
    if results:
        sheets.append({
            "title": "Realized Trades",
            "rows": results,
            "headers": list(results[0].keys()) if results else []
        })
    if open_positions:
        sheets.append({
            "title": "Open Positions",
            "rows": open_positions,
            "headers": list(open_positions[0].keys()) if open_positions else []
        })

    # Add new dividend sheets if data exists
    if dividends_nap:
        sheets.append({
            "title": "dividends-nap-autopilot",
            "rows": dividends_nap,
            "headers": headers_nap_autopilot
        })
    if dividends_sheet:
        sheets.append({
            "title": "dividends-sheet",
            "rows": dividends_sheet,
            "headers": headers_dividends_sheet
        })
    if dividends_table:
        sheets.append({
            "title": "dividends-table",
            "rows": dividends_table,
            "headers": headers_dividends_table
        })

    # Add new Interest sheet if data exists
    if interest_data:
        sheets.append({
            "title": "Interest",
            "rows": interest_data,
            "headers": headers_interest
        })

    if not sheets:
        print("\nNo data to write to ODS. Exiting.")
        sys.exit(0)

    # Write the ODS file with all collected sheets
    write_ods_with_totals(xml_dir, sheets, output_file_name)

    print("\nProcessing complete. Check the generated ODS file.")

if __name__ == "__main__":
    main()
