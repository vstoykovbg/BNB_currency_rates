#!/usr/bin/python3

import argparse
import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import calendar
import random
import time
import sys
from decimal import Decimal, getcontext
from decimal import Decimal, InvalidOperation

def download_and_process_exchange_rate_data(year, currency):
    def get_month_start_end_dates(year, month):
        """Returns the first and last day of a given month in a year."""
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, calendar.monthrange(year, month)[1])
        return first_day, last_day

    def validate_first_line(first_line, start_date, end_date):
        """Validates the first line of the CSV data."""
        # Join the first line if it was split by commas
        if isinstance(first_line, list):
            first_line = ",".join(first_line).strip()  # No extra space after the comma
        
        # Remove any leading/trailing whitespace or invisible characters (e.g., BOM)
        first_line = first_line.strip()
        print(f"first_line: \"{first_line}\"")  # Debugging output

        expected_start = "Курсове на българския лев"
        expected_end = f"{start_date.strftime('%d.%m.%Y')} до {end_date.strftime('%d.%m.%Y')}"
        
        # Check if the expected start string is present in the first line
        if expected_start not in first_line:
            raise ValueError(f"First line does not contain '{expected_start}'.")
        
        # Check if the expected end string is present in the first line
        if expected_end not in first_line:
            raise ValueError(f"First line does not contain the expected date range '{expected_end}'.")

    def validate_headers(headers):
        """Validates the headers of the CSV data."""
        if len(headers) < 5:
            raise ValueError("CSV headers are incomplete.")
        
        if headers[2].strip() != "за":
            raise ValueError(f"Third column header is not 'за'. Found: '{headers[2]}'.")
        if headers[3].strip() != "в BGN":
            raise ValueError(f"Fourth column header is not 'в BGN'. Found: '{headers[3]}'.")

    def download_data(start_date, end_date, currency):
        """Downloads exchange rate data for a given date range."""
        url = (
            f"https://www.bnb.bg/Statistics/StExternalSector/StExchangeRates/StERForeignCurrencies/index.htm?"
            f"downloadOper=true&group1=second&periodStartDays={start_date.day:02d}&periodStartMonths={start_date.month:02d}&periodStartYear={start_date.year}"
            f"&periodEndDays={end_date.day:02d}&periodEndMonths={end_date.month:02d}&periodEndYear={end_date.year}&valutes={currency}&search=true"
            f"&showChart=false&showChartButton=true&type=CSV"
        )

        start_date_str = start_date.strftime("%d %B %Y")  # "01 January 2023"
        end_date_str = end_date.strftime("%d %B %Y")      # "31 December 2023"

        print(f"Preparing to download currency rates for {currency} from {start_date_str} to {end_date_str}...")

        # Generate a random sleep duration between 1000 and 3000 milliseconds
        sleep_duration_ms = random.randint(1000, 3000)
        sleep_duration_s = sleep_duration_ms / 1000  # Convert milliseconds to seconds

        print(f"Sleeping for {sleep_duration_s:.3f} seconds... ", end="")
        sys.stdout.flush()  # This will force the output to be printed immediately
        time.sleep(sleep_duration_s)
        print("Done sleeping.")

        # Fetch data from the URL
        print("Fetching data... ", end="")
        sys.stdout.flush()  # This will force the output to be printed immediately
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from {url} with status code {response.status_code}")
        print("Data fetched successfully.")

        # Parse the CSV data
        csv_content = StringIO(response.text)
        reader = csv.reader(csv_content, delimiter=',')

        # Validate the first line
        first_line = next(reader)  # Get the entire first row (split by commas)
        validate_first_line(first_line, start_date, end_date)

        # Validate the headers
        headers = next(reader)
        validate_headers(headers)

        # Return only the data lines (skip the first two lines)
        data_lines = list(reader)
        return data_lines

    def parse_csv_data(data_lines, currency):
        """Parses the CSV data and returns a dictionary of dates and rates."""
        rates = {}

        for row in data_lines:
            # Skip invalid rows (e.g., headers, empty rows)
            if len(row) < 5 or not row[0].strip().replace(".", "").isdigit():
                continue

            try:
                date = row[0].strip()
                currency_code = row[1].strip()
                quantity_str = row[2].strip()
                rate_in_bgn_str = row[3].strip()

                # Skip rows where quantity or rate is "n/a"
                if quantity_str.lower() == "n/a" or rate_in_bgn_str.lower() == "n/a":
                    print("Warning: n/a in the data for currency ", currency_code ," at date", date, ".")
                    continue

                quantity = Decimal(quantity_str)  # The "за" column (X units)
                rate_in_bgn = Decimal(rate_in_bgn_str)  # The "в BGN" column (BGN for X units)
                
                # Check if the currency code matches
                if currency_code != currency:
                    raise ValueError(f"Expected currency code '{currency}' but found '{currency_code}' on {date}.")

                # Calculate rate for 1 unit: BGN per 1 currency unit
                rate_per_1_unit = rate_in_bgn / quantity
                rates[date] = rate_per_1_unit
            except (ValueError, IndexError, ZeroDivisionError, InvalidOperation):
                print("Warning: exception in parse_csv_data.")
                continue  # Ignore rows with invalid data
        
        return rates

    def fill_gaps_with_previous_rate(rates, start_date, end_date):
        """Fills missing dates in the rates dictionary using the last available rate."""
        all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        filled_rates = {}
        last_rate = None

        for date in all_dates:
            date_str = date.strftime("%d.%m.%Y")
            if date_str in rates:
                last_rate = rates[date_str]
            if last_rate is not None:
                filled_rates[date_str] = last_rate
            else:
                print(f"Warning: No rate available for {date_str} and earlier dates.")
        
        return filled_rates

    # Download data for December of the previous year through to December of the current year
    december_start, december_end = get_month_start_end_dates(year - 1, 12)
    current_year_start, current_year_end = get_month_start_end_dates(year, 12)

    # Download data for the entire period from December of the previous year to December of the current year
    all_data = []
    for month in range(12, 13):  # Only December from the previous year
        start_date, end_date = get_month_start_end_dates(year - 1, month)
        december_data = download_data(start_date, end_date, currency)
        if december_data:
            all_data.append(december_data)
        else:
            print(f"Warning: No data available for December {year - 1}.")

    for month in range(1, 13):  # For each month of the current year
        start_date, end_date = get_month_start_end_dates(year, month)
        monthly_data = download_data(start_date, end_date, currency)
        if monthly_data:
            all_data.append(monthly_data)
        else:
            print(f"Warning: No data available for {calendar.month_name[month]} {year}.")
    
    # Combine all data into a single list
    combined_rates = {}
    for data_lines in all_data:
        parsed_rates = parse_csv_data(data_lines, currency)
        combined_rates.update(parsed_rates)

    # Fill gaps for the entire period from December of the previous year to December of the current year
    first_date = datetime(year - 1, 12, 1)
    last_date = datetime(year, 12, 31)

    filled_rates = fill_gaps_with_previous_rate(combined_rates, first_date, last_date)

    # Filter rates to only include the desired year
    final_rates = {date: rate for date, rate in filled_rates.items() if datetime.strptime(date, "%d.%m.%Y").year == year}

    return final_rates


def save_rates_to_csv(rates, filename):
    """Saves the rates dictionary to a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Exchange Rate'])
        for date, rate in rates.items():
            # Format the rate to remove trailing zeros and unnecessary decimal points
            formatted_rate = format(rate.normalize(), 'f').rstrip('0').rstrip('.')
            writer.writerow([date, formatted_rate])

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download and process currency exchange rates")
    parser.add_argument('currency', help="Currency code (e.g., GBP, USD)")
    parser.add_argument('year', type=int, help="Year for which to download the data (e.g., 2024)")
    parser.add_argument('output_file', help="Output CSV file to save the rates")

    args = parser.parse_args()

    # Download and process exchange rate data
    rates = download_and_process_exchange_rate_data(args.year, args.currency)

    # Check if rates are None or empty
    if rates is None or not rates:
        print("Failed to download or no rates available.")
        return  # Exit the function early if rates are empty or None

    # Save the processed rates to the specified CSV file
    save_rates_to_csv(rates, args.output_file)
    print(f"Exchange rates for {args.currency} in {args.year} saved to {args.output_file}")
    
if __name__ == "__main__":
    main()
