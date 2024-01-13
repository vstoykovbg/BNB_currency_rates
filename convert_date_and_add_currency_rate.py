#!/usr/bin/python3

import csv
from datetime import datetime

def convert_date(date_str):
    try:
        # Try parsing as YYYY-MM-DD format
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        # If not in YYYY-MM-DD format, try parsing as MM/DD/YY format
        date_obj = datetime.strptime(date_str, '%m/%d/%y')

    # Format the date as DD.MM.YYYY
    return date_obj.strftime('%d.%m.%Y')

def main(csv_file, input_file, output_file):
    try:
        # Read currency rates from CSV database
        with open(csv_file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            currency_rates = {row[0]: row[1] for row in csv_reader}

        # Read dates from input file
        with open(input_file, 'r') as file:
            input_dates = file.readlines()

        # Write results to output file
        with open(output_file, 'w') as file:
            file.write("Original Date,Converted Date,Currency Rate\n")
            for input_date in input_dates:
                input_date = input_date.strip()
                converted_date = convert_date(input_date)
                currency_rate = currency_rates.get(converted_date, "N/A")
                file.write(f"{input_date},{converted_date},{currency_rate}\n")

        print(f"Conversion completed. Results written to {output_file}")

    except FileNotFoundError:
        print(f"Error: File not found - {csv_file}, {input_file}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: convert_date_and_add_currency_rate.py currency_rates.csv input_file output_file.csv")
    else:
        csv_file = sys.argv[1]
        input_file = sys.argv[2]
        output_file = sys.argv[3]
        main(csv_file, input_file, output_file)
