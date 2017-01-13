import json
import logging
import argparse
import os
from datetime import timedelta
import pickle
import math

import itertools
import shelve

import pandas
import numpy

import cointeg


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
        eod_data = load_security(eod_path, exchange, stock, excludes_func=excludes)
        if eod_data is not None:
            close_df['%s/%s' % (exchange, stock)] = eod_data['close_adj']
            volume_df['%s/%s' % (exchange, stock)] = eod_data['volume']
            logging.info('processed: %s/%s %d/%d' % (exchange, stock, count + 1, len(securities)))

    logging.info("result:\n%s" % close_df)
    close_df.to_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df.to_pickle(os.sep.join([eod_path, 'volume.pkl']))


def ncr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)


def generate_data(eod_path):
    close_df = pandas.read_pickle(os.sep.join([eod_path, 'eod.pkl']))
    volume_df = pandas.read_pickle(os.sep.join([eod_path, 'volume.pkl']))
    recent_history = volume_df.index.max() - timedelta(days=60)
    median_volumes = volume_df[volume_df.index > recent_history].fillna(0).median()
    most_traded = median_volumes[median_volumes > 1E5].keys().values.tolist()
    columns = [column for column in most_traded if column.startswith('PCX/')]
    logging.info('combining %d series' % len(columns))

    def to_key(symbols):
        return str(symbols)

    total = ncr(len(columns), 3)
    logging.info('Possible combinations: %d' % total)

    results = dict()

    last_completed = None
    for count, combination in enumerate(itertools.combinations(columns, 3)):
        if int((count / total) * 100) % 5 == 0 and int((count / total) * 100) != last_completed:
            last_completed = int((count / total) * 100)
            logging.info('%d%% completed' % last_completed)

        if to_key(combination) not in results:
            current_df = close_df[list(combination)].dropna()
            try:
                result = cointeg.cointegration_johansen(current_df)
                keepers = ('eigenvectors', 'trace_statistic', 'eigenvalue_statistics', 'critical_values_trace', 'critical_values_max_eigenvalue')
                results[to_key(combination)] = dict((k, result[k]) for k in keepers if k in result)

            except Exception as err:
                logging.error('failed for combination: %s' % str(combination))

    with open(os.sep.join([eod_path, 'results']), 'wb') as handle:
        pickle.dump(results, handle, protocol=pickle.HIGHEST_PROTOCOL)


def analyse(eod_path):
    by_eigenvalue_stats = dict()
    by_trace_stats = dict()
    with shelve.open(os.sep.join([eod_path, 'results'])) as results:
        total = len(results.keys())
        for count, combination in enumerate(results.keys()):

            if 'PCX/GLD' in combination and 'PCX/IAU' in combination:
                continue

            if 'PCX/DBEF' in combination and 'PCX/HEFA' in combination:
                continue

            if 'PCX/IJH' in combination and 'PCX/MDY' in combination:
                continue

            if 'PCX/BKLN' in combination and 'PCX/HYG' in combination:
                continue

            # DANGEROUS
            if 'PCX/VIXY' in combination:
                continue
            if 'PCX/VXX' in combination:
                continue
            if 'PCX/OIL' in combination:
                continue

            # LEVERAGED
            if 'PCX/LABU' in combination:
                continue
            if 'PCX/LABD' in combination:
                continue
            if 'PCX/NUGT' in combination:
                continue
            if 'PCX/EDZ' in combination:
                continue
            if 'PCX/EDC' in combination:
                continue
            if 'PCX/JDST' in combination:
                continue
            if 'PCX/JNUG' in combination:
                continue
            if 'PCX/TMF' in combination:
                continue
            if 'PCX/TMV' in combination:
                continue

            if combination not in results.keys():
                continue

            stats = results[combination]
            eigenvalue_stats = stats['eigenvalue_statistics']
            trace_stats = stats['trace_statistic']
            if trace_stats[0] > 25:
                by_eigenvalue_stats[eigenvalue_stats[0]] = (combination, results[combination])
                by_trace_stats[trace_stats[0]] = (combination, results[combination])

            if count % 10000 == 9999:
                logging.info('processed %d / %d' % (count + 1, total))

    trace_stats = sorted(by_trace_stats.keys())
    trace_df = pandas.DataFrame(trace_stats)
    logging.info(trace_df.describe())
    logging.info('best results')
    for count in range(min(50, len(trace_stats))):
        portfolio, result = by_trace_stats[trace_stats[-count]]
        weights = result['eigenvectors'].transpose()[0]
        logging.info('portfolio: %s' % portfolio)
        logging.info('normalized weights: %s' % numpy.divide(weights, numpy.absolute(weights).min()))
        logging.info('stats eigenvalues: %s' % result['eigenvalue_statistics'])
        logging.info('critical eigenvalues: %s' % result['critical_values_max_eigenvalue'])
        logging.info('stats trace: %s' % result['trace_statistic'])
        logging.info('critical trace: %s' % result['critical_values_trace'])


def main(args):
    eod_path = args.eod_path
    logging.info('using eod path: %s' % os.path.abspath(eod_path))
    if eod_path is None:
        eod_path = '.'

    if args.prepare:
        prepare_data(eod_path)

    if args.generate:
        generate_data(eod_path)

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
    parser.add_argument('--prepare', dest='prepare', action='store_true', help='preparing data')
    parser.add_argument('--generate', dest='generate', action='store_true', help='generating results')
    parser.add_argument('--analyse', dest='analyse', action='store_true', help='analysing results')
    args = parser.parse_args()
    main(args)
