import logging
import os
import csv
import pandas
from datetime import date


def load_prices(prices_path, exchange, security_code):
    letter = security_code[0]
    dir_path = os.sep.join([prices_path, exchange, letter, security_code])
    logging.info('accessing prices from: %s' % str(os.path.abspath(dir_path)))
    prices_df = None
    for root, dir, files in os.walk(dir_path):
        csv_files = [csv_file for csv_file in files if csv_file.endswith('.csv')]
        for csv_file in sorted(csv_files):
            with open(os.sep.join([root, csv_file]), 'r') as csv_data:
                fields = ['date', 'open', 'high', 'low', 'close', 'close adj', 'volume']
                reader = csv.DictReader(csv_data, fieldnames=fields)
                data = list()
                type_fields = {
                    'date': lambda yyyymmdd: date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])),
                    'open': float, 'high': float, 'low': float, 'close': float, 'close adj': float,
                    'volume': int
                }
                for row in reader:
                    converted_row = {key: type_fields[key](row[key]) for key in row}
                    data.append(converted_row)

                if prices_df is None:
                    prices_df = pandas.DataFrame(data)
                    prices_df.set_index('date')

                else:
                    prices_df = prices_df.append(data, ignore_index=True)

    dividends = prices_df['close'].shift(1) * prices_df['close adj'] / prices_df['close adj'].shift(1) - prices_df['close']
    prices_df['dividend'] = dividends[dividends > 1E-4]
    return prices_df.set_index('date')

