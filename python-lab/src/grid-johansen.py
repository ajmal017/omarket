import logging
import argparse
import os
import pandas


def load_security(path_db, exchange, symbol):
    data_path = os.sep.join([path_db, exchange, symbol[0], symbol])
    logging.info('path=' + os.path.abspath(data_path))
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
        path = root.split(os.sep)
        for file in files:
            if file.endswith('.csv'):
                exchange = path[-3]
                stock = path[-1]
                yield exchange, stock


def main(args):
    eod_path = args.eod_path
    if eod_path is None:
        eod_path = '.'

    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    for count, data in enumerate(list_securities(eod_path)):
        exchange, stock = data
        logging.info('%s/%s' % (exchange, stock))
        if count == 10:
            break

    eod_data = load_security(eod_path, 'PCX', 'HYG')
    logging.info("result:\n%s" % eod_data)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('grid-johansen.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    parser = argparse.ArgumentParser(description='Grid using johansen test.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--eod-path', type=str, help='path to eod data')
    args = parser.parse_args()
    main(args)
