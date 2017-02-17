import csv
import logging
import argparse
import os
from datetime import timedelta
import pickle
import math

import itertools

import pandas
import numpy

import cointeg

_ETFS = (
    'SCHF', 'AMJ', 'FEZ', 'EMB', 'EWC', 'UUP', 'EWY', 'IWF', 'HEDJ', 'VOO', 'BND', 'VEU', 'ITB', 'IWD',
    'DBC', 'VTI', 'EWG', 'USMV', 'EWH', 'PGX', 'EPI', 'IEFA', 'AGG', 'KBE', 'VGK', 'DIA', 'IVV',
    'PFF', 'EWW', 'VNQ', 'XME', 'XLB', 'BKLN', 'XLY', 'XRT', 'LQD', 'XBI', 'DXJ', 'IEMG', 'GLD',
    'KRE', 'SLV', 'IYR', 'XLV', 'AMLP', 'VEA', 'XLK', 'IAU', 'RSX',
    'XLI', 'JNK', 'HYG', 'XLE', 'XOP', 'VWO', 'XLP', 'XLU', 'FXI', 'EWZ', 'EFA',
    'UNG', 'GDXJ', 'IWM', 'USO', 'EEM', 'GDX', 'SPY', 'XLF'
)


def load_security(path_db, exchange, symbol, excludes_func=None):
    data_path = os.sep.join([path_db, exchange, symbol[0], symbol])
    dataframes = list()
    header = ('date', 'open', 'high', 'low', 'close', 'close_adj', 'volume')
    date_parser = lambda x: pandas.datetime.strptime(x, '%Y%m%d')
    description = open(os.sep.join([data_path, 'name.txt']), 'r').read()
    if excludes_func:
        if excludes_func(description):
            logging.info('excluding: %s' % description)
            return None

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
    """
    Filtering out unwanted ETFs (leveraged)
    :param eod_path:
    :return:
    """
    securities = list()
    for count, data in enumerate(list_securities(eod_path)):
        exchange, stock = data
        securities.append((exchange, stock))

    close_df = pandas.DataFrame()
    volume_df = pandas.DataFrame()

    def excludes(name):
        result = False
        result |= 'X2' in name.upper()
        result |= 'X3' in name.upper()
        result |= '3X' in name.upper()
        result |= '2X' in name.upper()
        result |= 'ULTRA' in name.upper()

        return result

    for count, data in enumerate(securities):
        exchange, stock = data
        if not stock in _ETFS:
            continue

        eod_data = load_security(eod_path, exchange, stock, excludes_func=excludes)
        if eod_data is not None:
            close_df['%s/%s' % (exchange, stock)] = eod_data['close_adj']
            volume_df['%s/%s' % (exchange, stock)] = eod_data['volume']
            logging.info('processed: %s/%s %d/%d' % (exchange, stock, count + 1, len(securities)))

    logging.info("result:\n%s" % close_df)
    close_df.to_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df.to_pickle(os.sep.join([eod_path, 'volume.pkl']))


def ncr(total, samples):
    f = math.factorial
    return f(total) // f(samples) // f(total - samples)


def generate_data(eod_path, number_securities=3):

    close_df = pandas.read_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df = pandas.read_pickle(os.sep.join([eod_path, 'volume.pkl']))
    recent_history = volume_df.index.max() - timedelta(days=60)
    median_volumes = volume_df[volume_df.index > recent_history].fillna(0).median()
    most_traded = median_volumes[median_volumes > 1E5].keys().values.tolist()
    columns = [column for column in most_traded if column.startswith('PCX/')]
    logging.info('combining %d series' % len(columns))

    def to_key(symbols):
        return ','.join(symbols)

    total = ncr(len(columns), number_securities)
    logging.info('Possible combinations: %d' % total)

    results = dict()

    last_completed = None
    for count, combination in enumerate(itertools.combinations(columns, number_securities)):
        # Almost identical leads to untrade-able discrepancies
        if 'PCX/IAU' and 'PCX/GLD' in combination:
            continue

        if int((count / total) * 100) % 5 == 0 and int((count / total) * 100) != last_completed:
            last_completed = int((count / total) * 100)
            logging.info('%d%% completed' % last_completed)

        if to_key(combination) not in results:
            current_df = close_df[list(combination)].dropna()
            try:
                result = cointeg.cointegration_johansen(current_df)
                keepers = ('eigenvectors', 'trace_statistic', 'eigenvalue_statistics', 'critical_values_trace',
                           'critical_values_max_eigenvalue')
                results[to_key(combination)] = dict((k, result[k]) for k in keepers if k in result)

            except Exception as err:
                logging.error('failed for combination: %s' % str(combination))

    with open(os.sep.join([eod_path, 'results.pkl']), 'wb') as handle:
        pickle.dump(results, handle, protocol=pickle.HIGHEST_PROTOCOL)


def analyse(eod_path):
    all_eigenvalue_stats = list()
    all_trace_stats = list()
    with open(os.sep.join([eod_path, 'results.pkl']), 'rb') as handle:
        results = pickle.load(handle)
        total = len(results.keys())
        for count, combination in enumerate(results.keys()):
            stats = results[combination]
            eigenvalue_stats = stats['eigenvalue_statistics']
            trace_stats = stats['trace_statistic']
            if trace_stats[0] > stats['critical_values_trace'][0][0] and eigenvalue_stats[0] > stats['critical_values_max_eigenvalue'][0][0]:
                all_eigenvalue_stats.append((eigenvalue_stats[0], combination, results[combination]))
                all_trace_stats.append((eigenvalue_stats[0], combination, results[combination]))

            if count % 10000 == 9999:
                logging.info('processed %d / %d' % (count + 1, total))

    all_trace_stats.sort(key=lambda x: -x[0])
    trace_df = pandas.DataFrame(all_trace_stats)
    logging.info(trace_df.describe())
    result_file = os.sep.join(['..', '..', 'data', 'results.csv'])
    logging.info('writing best results to: %s' % os.path.abspath(result_file))

    with open(result_file, mode='w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        for count in range(len(all_trace_stats)):
            stat, portfolio, result = all_trace_stats[count]
            portfolio = [name.split('/')[1] for name in portfolio.replace("'", '').replace('(', '').replace(')', '').split(',')]
            weights = result['eigenvectors'].transpose()[0]
            norm_weights = [round(x, 2) for x in numpy.divide(weights, numpy.absolute(weights).min()).tolist()]
            logging.debug('portfolio: %s' % str(portfolio))
            logging.debug('normalized weights: %s' % norm_weights)
            logging.debug('stat trace: %s' % result['trace_statistic'][0])
            logging.debug('critical trace : %s' % result['critical_values_trace'][0][0])
            logging.debug('stat eigenvalue: %s' % result['eigenvalue_statistics'][0])
            logging.debug('critical eigenvalue : %s' % result['critical_values_max_eigenvalue'][0][0])
            stat_fields = result['trace_statistic'][0], result['critical_values_trace'][0][0], result['eigenvalue_statistics'][0], result['critical_values_max_eigenvalue'][0][0]
            csvwriter.writerow(portfolio + norm_weights + list(stat_fields))


def main(args):
    eod_path = args.eod_path
    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    if eod_path is None:
        eod_path = '.'

    if args.prepare:
        prepare_data(eod_path)

    if args.generate:
        generate_data(eod_path, args.number_securities)

    if args.analyse:
        analyse(eod_path)


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
    parser.add_argument('--number-securities', type=int, help='number of securities', default=3)
    parser.add_argument('--prepare', dest='prepare', action='store_true', help='preparing data')
    parser.add_argument('--generate', dest='generate', action='store_true', help='generating results')
    parser.add_argument('--analyse', dest='analyse', action='store_true', help='analysing results')
    args = parser.parse_args()
    main(args)
