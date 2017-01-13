import argparse
import json
import logging
import os
from datetime import timedelta
import pandas


def map_ibrokers(contracts_path):
    to_ib = dict()
    for root, dirs, files in os.walk(contracts_path, topdown=False):
        for filename in files:
            if filename.endswith('.json'):
                details = json.loads(open(os.sep.join([root, filename])).read())
                if 'm_contract' not in details:
                    continue

                exchSymbol = details['m_contract']['m_primaryExch'], details['m_contract']['m_localSymbol']
                to_ib[exchSymbol] = details
    return to_ib


def main(args):
    eod_path = '../data/eod'
    contracts_path = '../data/contracts'
    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    volume_df = pandas.read_pickle(os.sep.join([eod_path, 'volume.pkl']))
    recent_history = volume_df.index.max() - timedelta(days=60)
    median_volumes = volume_df[volume_df.index > recent_history].fillna(0).median()
    median_volumes = median_volumes.sort_values(inplace=False).tail(80)
    most_traded = median_volumes[median_volumes > 5E5].keys().values.tolist()
    columns = [column for column in most_traded if column.startswith('PCX/')]
    to_ib = map_ibrokers(contracts_path)

    for column in columns:
        exchange, symbol = column.split('/')
        stock_path = os.sep.join([eod_path, exchange, symbol[0], symbol, 'name.txt'])
        name = open(stock_path, 'r').read()
        details = to_ib[('ARCA', symbol)]
        ib_name = details['m_longName']
        ib_code = details['m_contract']['m_conid']
        logging.info('%s,%s,%s,%s' % (ib_code, ib_name, name, symbol))

    logging.info('combining %d series' % len(columns))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('draft.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='draft script.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    args = parser.parse_args()
    main(args)
