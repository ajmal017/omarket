import logging
import argparse
import os
from datetime import timedelta
from matplotlib import pyplot

import itertools
import shelve

import pandas

import cointeg


def load_security(path_db, exchange, symbol):
    data_path = os.sep.join([path_db, exchange, symbol[0], symbol])
    dataframes = list()
    header = ('date', 'open', 'high', 'low', 'close', 'close_adj', 'volume')
    date_parser = lambda x: pandas.datetime.strptime(x, '%Y%m%d')
    for root, dirs, files in os.walk(data_path):
        for year_file in [f for f in files if f.endswith('.csv')]:
            dataframe = pandas.read_csv(os.sep.join([root, year_file]),
                                        names=header,
                                        parse_dates=['date'],
                                        date_parser=date_parser)
            dataframe.set_index('date', inplace=True)
            dataframes.append(dataframe)

    return pandas.concat(dataframes, axis=0)


def list_securities(path):
    for root, dirs, files in os.walk(path):
        if len(path) >= 3 and len([f for f in files if f.endswith('.csv')]) > 0:
            path = root.split(os.sep)
            exchange = path[-3]
            stock = path[-1]
            yield exchange, stock


def prepare_data(eod_path):
    securities = list()
    for count, data in enumerate(list_securities(eod_path)):
        exchange, stock = data
        securities.append((exchange, stock))

    close_df = pandas.DataFrame()
    volume_df = pandas.DataFrame()
    for count, data in enumerate(securities):
        exchange, stock = data
        eod_data = load_security(eod_path, exchange, stock)
        close_df['%s/%s' % (exchange, stock)] = eod_data['close_adj']
        volume_df['%s/%s' % (exchange, stock)] = eod_data['volume']
        logging.info('processed: %s/%s %d/%d' % (exchange, stock, count + 1, len(securities)))

    logging.info("result:\n%s" % close_df)
    close_df.to_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df.to_pickle(os.sep.join([eod_path, 'volume.pkl']))


def generate_data(eod_path):
    pyplot.style.use('ggplot')
    close_df = pandas.read_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df = pandas.read_pickle(os.sep.join([eod_path, 'volume.pkl']))
    recent_history = volume_df.index.max() - timedelta(days=60)
    median_volumes = volume_df[volume_df.index > recent_history].fillna(0).median()
    most_traded = median_volumes[median_volumes > 1E6].keys().values.tolist()
    columns = [column for column in most_traded if column.startswith('PCX/')]
    logging.info('combining %d series' % len(columns))

    def to_key(symbols):
        return str(symbols)

    for count, combination in enumerate(itertools.combinations(columns, 5)):
        with shelve.open(os.sep.join([eod_path, 'results'])) as results:
            if to_key(combination) not in results:
                logging.info('processing: %s' % str(combination))
                current_df = close_df[list(combination)].dropna()
                try:
                    result = cointeg.cointegration_johansen(current_df)
                    keepers = ('eigenvectors', 'trace_statistic', 'eigenvalue_statistics', 'critical_values_trace', 'critical_values_max_eigenvalue')
                    results[to_key(combination)] = dict((k, result[k]) for k in keepers if k in result)

                except Exception as err:
                    logging.error('failed for combination: %s' % str(combination))

        if count == 100:
            break


def main(args):
    eod_path = args.eod_path
    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    if eod_path is None:
        eod_path = '.'

    if args.prepare:
        prepare_data(eod_path)

    if args.generate:
        generate_data(eod_path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('grid-johansen.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='Grid using johansen test.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--eod-path', type=str, help='path to eod data')
    parser.add_argument('--prepare', dest='prepare', action='store_true', help='prepare phase')
    parser.add_argument('--generate', dest='generate', action='store_true', help='generate phase')
    args = parser.parse_args()
    main(args)
