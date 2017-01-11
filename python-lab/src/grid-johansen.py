import logging
import argparse
import os

import itertools
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


def prepare_data(args):
    eod_path = args.eod_path
    if eod_path is None:
        eod_path = '.'

    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    securities = list()
    for count, data in enumerate(list_securities(eod_path)):
        exchange, stock = data
        securities.append((exchange, stock))

    df = pandas.DataFrame()
    for count, data in enumerate(securities):
        exchange, stock = data
        eod_data = load_security(eod_path, exchange, stock)
        df['%s/%s' % (exchange, stock)] = eod_data['close_adj']
        logging.info('processed: %s/%s %d/%d' % (exchange, stock, count + 1, len(securities)))

    logging.info("result:\n%s" % df)
    df.to_pickle(os.sep.join([eod_path, 'eod.pkl']))


def main(args):
    eod_path = args.eod_path
    if eod_path is None:
        eod_path = '.'

    df = pandas.read_pickle(os.sep.join([eod_path, 'eod.pkl']))
    columns = [column for column in df.columns if column.startswith('PCX/')]
    logging.info('combining %d series' % len(columns))
    for count, combination in enumerate(itertools.combinations(columns, 5)):
        current_df = df[list(combination)].dropna()
        result = cointeg.cointegration_johansen(current_df)
        trace_statistic = result['trace_statistic']
        eigen_vectors = result['eigenvectors']

        logging.info(trace_statistic)
        logging.info(eigen_vectors)
        if count == 10:
            break

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
    args = parser.parse_args()
    main(args)
