#!/usr/bin/python3

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

def main(input_file, output_file):
    try:
        # Read dates from input file
        with open(input_file, 'r') as file:
            lines = file.readlines()

        # Convert dates and write to output file
        with open(output_file, 'w') as file:
            for line in lines:
                converted_date = convert_date(line.strip())
                file.write(converted_date + '\n')

        print(f"Conversion completed. Results written to {output_file}")

    except FileNotFoundError:
        print(f"Error: File not found - {input_file}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: conver_date.py input_file output_file")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        main(input_file, output_file)

