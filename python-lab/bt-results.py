import argparse
import logging
import os
from collections import defaultdict

import click
import pandas


def main(args):
    results100 = pandas.read_csv(os.sep.join(['..', 'backtest-results-100.csv']))
    results200 = pandas.read_csv(os.sep.join(['..', 'backtest-results-200.csv']))
    results300 = pandas.read_csv(os.sep.join(['..', 'backtest-results-300.csv']))
    results100['lookback'] = 100
    results200['lookback'] = 200
    results300['lookback'] = 300
    results = pandas.concat([results100, results200, results300])
    summary = results.describe()
    logging.info('summary:\n%s', summary)
    stable_returns = (results['r-squared'] > results['r-squared'].quantile(0.95)) & (results['p-value F-test'] <= summary['p-value F-test']['25%'])
    high_sharpe = results['sharpe_ratio'] > summary['sharpe_ratio']['75%']
    often_traded = results['count_trades'] >= summary['count_trades']['75%']
    low_drawdown = results['max_drawdown_pct'] <= summary['max_drawdown_pct']['25%']
    results = results[(stable_returns & high_sharpe & often_traded & low_drawdown)]
    results = results.sort_values(by=['final_equity'], ascending=False)
    logging.info('filtered:\n%s', results)
    logging.info('filtered summary:\n%s', results.describe())
    selected_etfs = list()
    etf_occurences = defaultdict(int)
    for row, data in results.iterrows():
        portfolio = data['portfolio']
        lookback = int(data['lookback'])
        candidates = set(portfolio.split('/'))
        skip = False
        for etf in candidates:
            if etf_occurences[etf] >= 2:
                skip = True

        for etfs in selected_etfs:
            if not skip:
                if len(etfs.intersection(candidates)) >= 2:
                    skip = True

        if not skip:
            logging.info('portfolio: %d,%s' % (lookback, portfolio))
            selected_etfs.append(candidates)
            for etf in candidates:
                etf_occurences[etf] += 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('bt-results.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='Analysing backtest results',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    args = parser.parse_args()
    pandas.set_option('expand_frame_repr', False)
    main(args)