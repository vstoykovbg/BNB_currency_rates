#!/usr/bin/python3

import pandas as pd

input_file = 'USD_2023_with_gaps.csv'
output_file = 'USD_2023.csv'

df = pd.read_csv(input_file, header=None, names=['Date', 'Rate'], parse_dates=['Date'], dayfirst=True)

# Ensure the 'Date' column is in datetime format
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

# Create a DataFrame with all dates in the range, including weekends
date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
all_dates_df = pd.DataFrame({'Date': date_range})

# Merge the original DataFrame with the full date range DataFrame to fill in missing dates
result_df = pd.merge(all_dates_df, df, on='Date', how='left')

# Forward-fill missing values
result_df['Rate'].fillna(method='ffill', inplace=True)

# Write the result to another CSV file with the original date format
result_df.to_csv(output_file, index=False, date_format='%d.%m.%Y', float_format='%.5f')

print(f"Result has been written to {output_file}")

