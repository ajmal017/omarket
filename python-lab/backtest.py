import argparse
import csv
import logging
import math
import os
from datetime import date

import numpy
import pandas
from statsmodels.formula.api import OLS
from matplotlib import pyplot

from btplatform import PositionAdjuster, process_strategy, BacktestHistory
from meanrevert import MeanReversionStrategy, PortfolioDataCollector, StrategyDataCollector, \
    MeanReversionStrategyRunner
from pricetools import load_prices


def backtest_strategy(start_date, end_date, strategy_runner, symbols, prices_path):
    securities = ['PCX/' + symbol for symbol in symbols]
    prices_by_security = dict()
    close_prices = pandas.DataFrame()
    max_start_date = start_date
    min_end_date = end_date
    for security in securities:
        exchange, security_code = security.split('/')
        prices_df = load_prices(prices_path, exchange, security_code)
        prices_by_security[security] = prices_df
        if max_start_date is not None:
            max_start_date = max(max_start_date, prices_df.index.min())

        else:
            max_start_date = prices_df.index.min()

        if min_end_date is not None:
            min_end_date = min(min_end_date, prices_df.index.max())

        else:
            min_end_date = prices_df.index.max()

        close_prices[security] = prices_df['close adj']

    close_prices.reset_index(inplace=True)
    logging.info('considering date range: %s through %s' % (max_start_date, min_end_date))

    for security in securities:
        truncate_start_date = prices_by_security[security].index >= max_start_date
        truncate_end_date = prices_by_security[security].index <= min_end_date
        prices_by_security[security] = prices_by_security[security][truncate_start_date & truncate_end_date]

    data_collector = StrategyDataCollector(strategy_runner.get_strategy_name())
    process_strategy(securities, strategy_runner, data_collector, prices_by_security)
    return data_collector


def backtest_portfolio(portfolios, starting_equity, start_date, end_date, prices_path, step_size, max_net_position,
                       max_gross_position, max_risk_scale, warmup_period):
    data_collector = PortfolioDataCollector()
    for lookback_period, portfolio, strategy_name in portfolios:
        securities = portfolio.split('/')
        strategy = MeanReversionStrategy(securities, int(lookback_period), name=strategy_name)
        position_adjuster = PositionAdjuster(securities, strategy.get_strategy_name(), max_net_position,
                                             max_gross_position, max_risk_scale,
                                             starting_equity,
                                             step_size)
        strategy_runner = MeanReversionStrategyRunner(securities, strategy, warmup_period, position_adjuster)
        data_collection = backtest_strategy(start_date, end_date, strategy_runner, securities, prices_path)
        data_collector.add_equity(starting_equity)
        target_quantities = data_collection.get_target_quantities(strategy.get_strategy_name())
        fills = position_adjuster.get_fills()
        data_collector.add_strategy_data(securities, target_quantities, fills)

    return data_collector


def chart_backtest(start_date, end_date, securities, prices_path, lookback_period,
                   step_size, start_equity,
                   max_net_position, max_gross_position, max_risk_scale, warmup_period):
    pyplot.style.use('ggplot')
    strategy = MeanReversionStrategy(securities, int(lookback_period))
    position_adjuster = PositionAdjuster(securities, strategy.get_strategy_name(), max_net_position, max_gross_position,
                                         max_risk_scale,
                                         start_equity,
                                         step_size)
    strategy_runner = MeanReversionStrategyRunner(securities, strategy, warmup_period, position_adjuster)
    data_collection = backtest_strategy(start_date, end_date, strategy_runner, securities, prices_path)
    backtest_history = BacktestHistory(position_adjuster.get_fills(), start_equity)
    logging.info('fit quality: %s', fit_quality(backtest_history.get_equity() - start_equity))
    backtest_history.get_equity().plot(linewidth=2.)
    backtest_history.get_gross_net_position().plot(linewidth=2.)
    pyplot.gca().get_yaxis().get_major_formatter().set_useOffset(False)
    data_collection.get_factors(','.join(securities)).plot(linewidth=2., subplots=True)
    styles = {'level_inf': 'm--', 'level_sup': 'b--', 'signal': 'k-'}
    data_collection.get_bollinger(','.join(securities)).plot(linewidth=2., subplots=False, style=styles)
    pyplot.show()


def fit_quality(df):
    regr_df = df.reset_index()
    day_nanos = 24 * 60 * 60 * 1E9
    nanos = regr_df['date'] - regr_df['date'].min()
    df2 = pandas.DataFrame(data=[nanos.astype(int) / day_nanos, regr_df['equity']]).transpose()
    ols2 = OLS(df2['equity'], df2['date'])
    result = ols2.fit()
    return {'p-value F-test': result.f_pvalue, 'r-squared': result.rsquared, 'p-value x': result.pvalues[0]}


def create_summary(strategy_name, backtest_history, closed_trades):
        mean_trade = closed_trades['pnl'].mean()
        worst_trade = closed_trades['pnl'].min()
        count_trades = closed_trades['pnl'].count()
        max_drawdown = backtest_history.get_drawdown().max()['equity']
        final_equity = backtest_history.get_equity()['equity'][-1]
        summary = {
            'strategy': strategy_name,
            'sharpe_ratio': backtest_history.get_sharpe_ratio(),
            'average_trade': mean_trade,
            'worst_trade': worst_trade,
            'count_trades': count_trades,
            'max_drawdown_pct': max_drawdown,
            'final_equity': final_equity
        }
        return summary


def load_portfolios(portfolios_filename):
    portfolios = list()
    with open(portfolios_filename) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if len(row) == 0:
                continue

            if row[0].startswith('#'):
                continue

            portfolios.append(row)

    logging.info('loaded portfolios: %s' % str(portfolios))
    return portfolios


def main(args):
    # TODO arg line
    warmup_period = 10
    prices_path = args.prices_path
    start_date = date(int(args.start_yyyymmdd[:4]), int(args.start_yyyymmdd[4:6]), int(args.start_yyyymmdd[6:8]))
    end_date = date(int(args.end_yyyymmdd[:4]), int(args.end_yyyymmdd[4:6]), int(args.end_yyyymmdd[6:8]))
    if args.display_single is not None:
        securities = args.display_single.split('/')
        chart_backtest(start_date, end_date, securities, prices_path, lookback_period=args.lookback_period,
                       step_size=args.step_size, start_equity=args.starting_equity,
                       max_net_position=args.max_net_position,
                       max_gross_position=args.max_gross_position,
                       max_risk_scale=args.max_risk_scale, warmup_period=warmup_period)

    elif args.portfolio is not None:
        portfolios = load_portfolios(args.portfolio)
        step_size = args.step_size
        starting_equity = args.starting_equity
        max_net_position = args.max_net_position
        max_gross_position = args.max_gross_position
        max_risk_scale = args.max_risk_scale
        data_collector = backtest_portfolio(portfolios, starting_equity, start_date, end_date, prices_path, step_size,
                                            max_net_position, max_gross_position, max_risk_scale, warmup_period)
        backtest_history = BacktestHistory(data_collector.fills_df, data_collector.starting_equity)
        backtest_history.trades_pnl.to_pickle(os.sep.join([args.trades_pnl_path, 'trades_pnl.pkl']))

        trades = backtest_history.get_trades()
        holdings = backtest_history.get_holdings()
        equity = backtest_history.get_equity()
        target_df = data_collector.new_targets

        positions = holdings[['date', 'security', 'total_qty']].groupby(['date', 'security']).sum().unstack().ffill()
        latest_holdings = holdings.pivot_table(index='date', columns='security', values='total_qty',
                                               aggfunc=numpy.sum).tail(1).transpose()
        latest_holdings.columns = ['quantity']

        starting_equity = equity.iloc[0]
        ending_equity = equity.iloc[-1]
        days_interval = equity.index[-1] - equity.index[0]
        sharpe_ratio = math.sqrt(250) * equity.pct_change().mean() / equity.pct_change().std()
        logging.info('sharpe ratio: %.2f', sharpe_ratio)
        annualized_return = 100 * (numpy.power(ending_equity / starting_equity, 365 / days_interval.days) - 1)
        logging.info('annualized return: %.2f percent' % annualized_return)
        logging.info('trades:\n%s', trades.tail(10).transpose())
        logging.info('positions:\n%s', positions.tail(10).transpose())
        logging.info('new target quantities:\n%s' % (target_df))
        target_trades = (target_df - latest_holdings.transpose()).transpose().dropna()
        logging.info('future trades:\n%s' % target_trades.round())

    elif args.display_portfolio is not None:
        portfolios = load_portfolios(args.display_portfolio)

        pyplot.style.use('ggplot')
        trades_pnl_df = pandas.read_pickle(os.sep.join([args.trades_pnl_path, 'trades_pnl.pkl']))
        backtest_history = BacktestHistory(trades_pnl_df)
        backtest_history.set_start_equity(len(portfolios) * args.starting_equity)

        pnl_data = backtest_history.trades_pnl[['strategy', 'date', 'realized_pnl', 'unrealized_pnl']]
        by_strategy_date = pnl_data.groupby(by=['strategy', 'date'])
        by_strategy_date.sum().apply(sum, axis=1).unstack().transpose().plot(linewidth=2., subplots=True, layout=(-1, 2))

        holdings = backtest_history.get_holdings()
        equity = backtest_history.get_equity()

        benchmark = load_prices(prices_path, 'PCX', 'SPY')
        equity_df = benchmark[['close adj']].join(equity).dropna()
        equity_df.columns = ['benchmark', 'equity']
        equity_df['benchmark'] = (equity_df['benchmark'].pct_change() + 1.).cumprod() * equity_df.head(1)[
            'equity'].min()
        equity_df.plot(linewidth=2.)
        logging.info('fit quality: %s', fit_quality(equity - args.starting_equity))
        by_security_pos = holdings.pivot_table(index='date', columns='security', values='market_value',
                                               aggfunc=numpy.sum)
        by_security_pos.plot(linewidth=2.)
        positions_aggregated_net = holdings.groupby('date')['market_value'].sum()
        positions_aggregated_gross = holdings.groupby('date')['market_value'].agg(lambda x: numpy.abs(x).sum())
        positions_net_gross = numpy.array([positions_aggregated_net, positions_aggregated_gross]).transpose()
        positions_aggregated = pandas.DataFrame(index=positions_aggregated_net.index,
                                                data=positions_net_gross,
                                                columns=['net', 'gross'])
        positions_aggregated = positions_aggregated.join(equity * 3.0)
        positions_aggregated.rename(columns={'equity': 'margin_warning'}, inplace=True)
        positions_aggregated = positions_aggregated.join(equity * 4.0)
        positions_aggregated.rename(columns={'equity': 'margin_violation'}, inplace=True)
        positions_aggregated.plot(linewidth=2., subplots=False)
        pyplot.show()

    elif args.batch is not None:
        # backtest batch
        portfolios_path = args.batch
        logging.info('processing batch: %s', os.path.abspath(portfolios_path))
        with open(portfolios_path) as portfolios_file:
            portfolios = [line.strip().split(',') for line in portfolios_file.readlines()]
            results = list()
            for symbols in portfolios:
                strategy = MeanReversionStrategy(symbols, int(args.lookback_period))
                position_adjuster = PositionAdjuster(symbols, strategy.get_strategy_name(), args.max_net_position,
                                                     args.max_gross_position,
                                                     args.max_risk_scale,
                                                     args.starting_equity,
                                                     args.step_size)
                strategy_runner = MeanReversionStrategyRunner(symbols, strategy, warmup_period, position_adjuster)
                backtest_strategy(start_date, end_date, strategy_runner, symbols, prices_path)
                backtest_history = BacktestHistory(position_adjuster.get_fills(), args.starting_equity)
                backtest_data = fit_quality(backtest_history.get_equity() - args.starting_equity)
                closed_trades = position_adjuster.get_strategy_trades(closed_only=True)
                backtest_data.update(create_summary(strategy.get_strategy_name(), backtest_history, closed_trades))
                results.append(backtest_data)

            result_df = pandas.DataFrame(results).set_index('strategy')
            result_df.to_csv('backtest-results.csv')
            print(result_df)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('backtest.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='Backtesting prototype.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--start-yyyymmdd', type=str, help='backtest start date', default='20130101')
    parser.add_argument('--end-yyyymmdd', type=str, help='backtest end date', default=date.today().strftime('%Y%m%d'))
    parser.add_argument('--display-single', type=str, help='display strategy composed of comma-separated securities')
    parser.add_argument('--display-portfolio', type=str, help='display aggregated portfolio from specified file')
    parser.add_argument('--portfolio', type=str, help='display aggregated portfolio from specified file')
    parser.add_argument('--batch', type=str, help='processes strategies in batch mode')
    parser.add_argument('--lookback-period', type=int, help='lookback period', default=200)
    parser.add_argument('--step-size', type=int, help='deviation unit measured in number of standard deviations',
                        default=2)
    parser.add_argument('--starting-equity', type=float,
                        help='amount of equity allocated to each strategy (for one risk step)', default=8000)
    parser.add_argument('--actual-equity', type=float, help='total equity available for trading')
    parser.add_argument('--max-net-position', type=float,
                        help='max allowed net position for one step, measured as a fraction of equity', default=0.4)
    parser.add_argument('--max-gross-position', type=float,
                        help='max allowed gross position by step, measured as a fraction of equity', default=2.)
    parser.add_argument('--max-risk-scale', type=int, help='max number of steps', default=3)
    parser.add_argument('--prices-path', type=str, help='path to prices data', default='data')
    parser.add_argument('--trades-pnl-path', type=str, help='path to trades pnl data', default='.')
    args = parser.parse_args()
    pandas.set_option('expand_frame_repr', False)
    main(args)
    # dev: --start-yyyymmdd 20170101 --end-yyyymmdd 20170313
