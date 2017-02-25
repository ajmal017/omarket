import argparse
import logging
import math

import numpy
import os
from datetime import date
from matplotlib import pyplot
import pandas
from pnl import AverageCostProfitAndLoss
from pricetools import load_prices
from regression import RegressionModelOLS


class ExecutionEngine(object):

    def __init__(self, securities):
        self._securities = securities
        self._securities_index = {security: count for count, security in enumerate(securities)}
        self._trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self._fills = list()

    def execute(self, timestamp, count, fill_qty, price):
        trades_tracker = self._trades_tracker[self._securities[count]]
        trades_tracker.add_fill(fill_qty, price)
        self._fills.append({'security': self._securities[count], 'date': timestamp, 'qty': fill_qty, 'price': price})

    def get_fills(self):
        return pandas.DataFrame(self._fills)

    def get_nav(self, prices):
        total_pnl = 0.
        for security in self._trades_tracker:
            pnl_calc = self._trades_tracker[security]
            price = prices[self._securities_index[security]]
            total_pnl += pnl_calc.get_total_pnl(price)

        return total_pnl


class PortfolioDataCollector(object):

    def __init__(self, position_adjuster):
        self.equity_history = list()
        self.net_position_history = list()
        self.gross_position_history = list()
        self.positions_history = list()
        self.holdings_history = list()
        self.chart_bollinger = list()
        self.chart_beta = list()
        self.chart_regression = list()
        self.position_adjuster = position_adjuster

    def historize_state(self, timestamp, signal_prices, market_prices):
        self.equity_history.append({'date': timestamp, 'equity': self.position_adjuster.get_nav(market_prices)})
        positions = self.position_adjuster.get_positions(signal_prices)
        net_positions = positions.sum()
        gross_positions = numpy.abs(positions).sum()
        self.net_position_history.append({'date': timestamp, 'net_position': net_positions})
        self.gross_position_history.append({'date': timestamp,
                                            'gross_position': gross_positions,
                                            'margin_call': self.position_adjuster.get_nav(market_prices) / 0.25,
                                            'margin_warning': self.position_adjuster.get_nav(market_prices) / 0.4})
        holdings = self.position_adjuster.get_holdings()
        for security in holdings:
            holdings_data = {'date': timestamp, 'strategy': self.position_adjuster.get_name()}
            holdings_data['quantity'] = holdings[security]
            holdings_data['security'] = security
            self.holdings_history.append(holdings_data)

        positions = self.position_adjuster.get_position_securities(signal_prices)
        for security in positions:
            positions_data = {'date': timestamp, 'strategy': self.position_adjuster.get_name()}
            positions_data['position'] = positions[security]
            positions_data['security'] = security
            self.positions_history.append(positions_data)

    def get_equity(self):
        return pandas.DataFrame(self.equity_history).set_index('date')

    def get_return(self):
        return self.get_equity().pct_change()

    def get_positions_history(self):
        return pandas.DataFrame(self.positions_history)

    def get_holdings_history(self):
        return pandas.DataFrame(self.holdings_history)

    def get_net_position(self):
        return pandas.DataFrame(self.net_position_history).set_index('date')

    def get_gross_position(self):
        return pandas.DataFrame(self.gross_position_history).set_index('date')

    def get_sharpe_ratio(self):
        mean_return = self.get_equity().pct_change().mean()
        std_return = self.get_equity().pct_change().std()
        value = mean_return / std_return * math.sqrt(250)
        return value['equity']

    def get_drawdown(self):
        cum_returns = (1. + self.get_return()).cumprod()
        return 1. - cum_returns.div(cum_returns.cummax())

    def add_bollinger(self, signal_data):
        self.chart_bollinger.append(signal_data)

    def add_factors(self, factors_data, day):
        beta_data = dict()
        for count_factor, weight in enumerate(factors_data):
            beta_data['beta%d' % count_factor] = weight

        beta_data['date'] = day
        self.chart_beta.append(beta_data)


class PositionAdjuster(object):

    def __init__(self, securities, max_net_position, max_gross_position, max_risk_scale, start_equity, step_size):
        self._securities = securities
        self._current_quantities = [0.] * len(securities)
        self._max_net_position = max_net_position
        self._max_gross_position = max_gross_position
        self._max_risk_scale = max_risk_scale
        self._step_size = step_size
        self._start_equity = start_equity
        self._current_risk_scale = 0.
        self._signal_zone = 0
        self._deviation = 0.
        self._open_trades = list()
        self._closed_trades = list()
        self._execution_engine = ExecutionEngine(securities)

    def execute_trades(self, timestamp, quantities, prices):
        for count, quantity_price in enumerate(zip(quantities, prices)):
            target_quantity, price = quantity_price
            fill_qty = target_quantity - self._current_quantities[count]
            self._execution_engine.execute(timestamp, count, fill_qty, price)
            self._current_quantities[count] = target_quantity

    def update_target_positions(self, timestamp, signal, deviation, weights, market_prices):
        self._deviation = deviation * self._step_size
        movement = 0
        if signal >= self.level_sup():
            movement = 1

        if signal <= self.level_inf():
            movement = -1

        self._signal_zone += movement
        quantities = None
        if movement != 0:
            scaling = self._update_risk_scaling(timestamp, weights, market_prices)
            quantities = list()
            for count, weight_price in enumerate(zip(weights, market_prices)):
                weight, price = weight_price
                target_position = scaling * self._current_risk_scale * weight
                target_quantity = round(target_position / price)
                quantities.append(target_quantity)

        return quantities

    def _update_risk_scaling(self, timestamp, weights, market_prices):
        target_risk_scale = self._signal_zone
        if abs(target_risk_scale) > self._max_risk_scale:
            logging.warning('risk scale %d exceeding max risk scale: capping to %d' % (target_risk_scale, self._max_risk_scale))
            if target_risk_scale > 0:
                target_risk_scale = self._max_risk_scale

            elif target_risk_scale < 0:
                target_risk_scale = -self._max_risk_scale

        if self._current_risk_scale == target_risk_scale:
            logging.warning('position already at specified risk scale: ignoring')
            return

        logging.debug('moving to position: %d at %s' % (target_risk_scale, timestamp.strftime('%Y-%m-%d')))
        scaling = 0
        if target_risk_scale != 0:
            equity = self.get_nav(market_prices)
            scaling_gross = equity * self._max_gross_position / numpy.max(numpy.abs(weights))
            scaling_net = equity * self._max_net_position / numpy.abs(numpy.sum(weights))
            scaling = min(scaling_net, scaling_gross)

        self._handle_trades(timestamp, target_risk_scale, market_prices)
        self._current_risk_scale = target_risk_scale
        return scaling

    def _handle_trades(self, timestamp, target_risk_scale, prices):
        """
        Keeps track of high-level trades.

        :param timestamp:
        :param target_risk_scale:
        :param prices:
        :return:
        """
        steps_count = int(abs(target_risk_scale) - abs(self._current_risk_scale))
        if steps_count > 0:
            logging.debug('opening %d trade(s)' % steps_count)
            strategy_equity = self.get_cumulated_pnl(prices)
            for trade_count in range(steps_count):
                self._open_trades.append({'open': timestamp,
                                         'risk_level': target_risk_scale,
                                         'equity_start': strategy_equity,
                                         'equity_end': strategy_equity,
                                         'pnl': 0.
                                          })

        else:
            logging.debug('closing %d trade(s)' % abs(steps_count))
            for trade_count in range(abs(steps_count)):
                trade_result = self._open_trades.pop()
                trade_result['close'] = timestamp
                self._closed_trades.append(trade_result)

            # when multiple risk levels attributes whole pnl to last one
            self._closed_trades[-1]['equity_end'] = self.get_cumulated_pnl(prices)
            self._closed_trades[-1]['pnl'] = self._closed_trades[-1]['equity_end'] - self._closed_trades[-1][
                'equity_start']

    def level_inf(self):
        return (self._signal_zone - 1) * self._deviation

    def level_sup(self):
        return (self._signal_zone + 1) * self._deviation

    def get_cumulated_pnl(self, prices):
        return self._execution_engine.get_nav(prices)

    def get_nav(self, prices):
        return self._start_equity + self.get_cumulated_pnl(prices)

    def get_positions(self, prices):
        return numpy.array(self._current_quantities) * prices

    def get_position_securities(self, prices):
        return dict(zip(self._securities, [position for position in self.get_positions(prices)]))

    def get_holdings(self):
        return dict(zip(self._securities, self._current_quantities))

    def get_name(self):
        return ','.join(self._securities)

    def get_fills(self):
        return self._execution_engine.get_fills()

    def get_closed_trades(self):
        return pandas.DataFrame(self._closed_trades)

    def get_open_trades(self):
        return pandas.DataFrame(self._open_trades)


class MeanReversionStrategy(object):
    def __init__(self, securities, lookback_period):
        #regression0 = RegressionModelFLS(securities, delta=5E-6, with_constant_term=False)
        self.regression = RegressionModelOLS(securities, with_constant_term=False, lookback_period=lookback_period)
        self.states = dict()

    def compute_signal(self, prices_close_adj):
        dependent_price, independent_prices = prices_close_adj[0], prices_close_adj[1:]
        self.regression.compute_regression(dependent_price, independent_prices.tolist())
        self.states['deviation'] = self.regression.get_residual_error()
        self.states['weights'] = self.regression.get_weights()
        self.states['signal'] = self.regression.get_residual()
        self.states['factors'] = self.regression.get_factors()

    def get_state(self, name):
        return self.states[name]


class MeanReversionStrategyRunner(object):

    def __init__(self, securities, strategy, warmup_period, position_adjuster, data_collector):
        self.securities = securities
        self.day = None
        self.last_phase = 'AfterClose'
        self.strategy = strategy
        self.warmup_period = warmup_period
        self.count_day = 0
        self.target_quantities = None
        self.position_adjuster = position_adjuster
        self.data_collector = data_collector

    def on_open(self, day, prices_open):
        assert self.last_phase == 'AfterClose'
        self.day = day
        self.count_day += 1

        if self.count_day > self.warmup_period:
            # on-open market orders
            if self.target_quantities is not None:
                self.position_adjuster.execute_trades(day, self.target_quantities, prices_open)

        self.last_phase = 'Open'

    def on_close(self, prices_close):
        assert self.last_phase == 'Open'
        self.last_phase = 'Close'

    def on_after_close(self, dividends, prices_close_adj, prices_close):
        assert self.last_phase == 'Close'
        self.strategy.compute_signal(prices_close_adj)
        self.data_collector.historize_state(self.day, prices_close_adj, prices_close)
        signal = self.strategy.get_state('signal')
        deviation = self.strategy.get_state('deviation')
        weights = self.strategy.get_state('weights')
        self.target_quantities = self.position_adjuster.update_target_positions(self.day, signal, deviation, weights, prices_close)
        self.last_phase = 'AfterClose'

    def collect_strategy_data(self):
        # statistics
        signal_data = {
            'date': self.day,
            'level_inf': self.position_adjuster.level_inf(),
            'level_sup': self.position_adjuster.level_sup(),
            'signal': self.strategy.get_state('signal')
        }
        self.data_collector.add_bollinger(signal_data)
        self.data_collector.add_factors(self.strategy.get_state('factors'), self.day)
        #self.data_collector.add_records()


def process_strategy(securities, strategy, warmup_period, prices_by_security,
                     step_size, start_equity, max_net_position, max_gross_position, max_risk_scale):
    """

    :param securities:
    :param strategy:
    :param warmup_period:
    :param prices_by_security:
    :param step_size:
    :param start_equity:
    :param max_net_position:
    :param max_gross_position:
    :param max_risk_scale:
    :return:
    """

    position_adjuster = PositionAdjuster(securities, max_net_position, max_gross_position, max_risk_scale, start_equity, step_size)
    dates = set()
    prices_open = pandas.DataFrame()
    prices_close = pandas.DataFrame()
    prices_close_adj = pandas.DataFrame()
    prices_dividend = pandas.DataFrame()
    for security in securities:
        security_prices = prices_by_security[security]
        prices_open = pandas.concat([prices_open, security_prices['open']])
        prices_close = pandas.concat([prices_close, security_prices['close']])
        prices_close_adj = pandas.concat([prices_close_adj, security_prices['close adj']])
        prices_dividend = pandas.concat([prices_dividend, security_prices['dividend']])
        dates = dates.union(set(security_prices.index.values.tolist()))

    data_collector = PortfolioDataCollector(position_adjuster)
    strategy_runner = MeanReversionStrategyRunner(securities, strategy, warmup_period, position_adjuster, data_collector)
    for count_day, day in enumerate(sorted(dates)):
        strategy_runner.on_open(day, prices_open[prices_open.index == day].values.transpose()[0])
        strategy_runner.on_close(prices_close[prices_close.index == day].values.transpose()[0])
        strategy_runner.on_after_close(prices_dividend[prices_dividend.index == day].values.transpose()[0],
                                       prices_close_adj[prices_close_adj.index == day].values.transpose()[0],
                                       prices_close[prices_close.index == day].values.transpose()[0]
                                       )
        strategy_runner.collect_strategy_data()

    closed_trades = position_adjuster.get_closed_trades()
    mean_trade = closed_trades['pnl'].mean()
    worst_trade = closed_trades['pnl'].min()
    count_trades = closed_trades['pnl'].count()
    max_drawdown = data_collector.get_drawdown().max()['equity']
    final_equity = data_collector.get_equity()['equity'][-1]
    summary = {
        'sharpe_ratio': data_collector.get_sharpe_ratio(),
        'average_trade': mean_trade,
        'worst_trade': worst_trade,
        'count_trades': count_trades,
        'max_drawdown_pct': max_drawdown,
        'final_equity': final_equity
    }
    logging.info('result: %s' % str(summary))
    result = {
        'summary': summary,
        'bollinger': pandas.DataFrame(data_collector.chart_bollinger).set_index('date'),
        'factors': pandas.DataFrame(data_collector.chart_beta).set_index('date'),
        'equity': data_collector.get_equity(),
        'net_position': data_collector.get_net_position(),
        'gross_position': data_collector.get_gross_position(),
        'positions': data_collector.get_positions_history(),
        'holdings': pandas.DataFrame(data_collector.get_holdings_history()),
        'fills': position_adjuster.get_fills(),
        'next_target_quantities': strategy_runner.target_quantities
    }
    return result


def backtest_portfolio(start_date, end_date, symbols, prices_path, lookback_period,
                       step_size, start_equity, max_net_position, max_gross_position,
                       max_risk_scale):
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
        prices_by_security[security] = prices_by_security[security][
            (prices_by_security[security].index >= max_start_date) & (prices_by_security[security].index <= min_end_date)]

    warmup_period = 10

    strategy = MeanReversionStrategy(securities, lookback_period)
    backtest_data = process_strategy(securities, strategy, warmup_period, prices_by_security,
                                     step_size=step_size, start_equity=start_equity,
                                     max_net_position=max_net_position,
                                     max_gross_position=max_gross_position,
                                     max_risk_scale=max_risk_scale)
    backtest_summary = backtest_data['summary']
    backtest_summary['portfolio'] = '/'.join(symbols)
    return backtest_data


def chart_backtest(start_date, end_date, securities, prices_path, lookback_period,
                   step_size, start_equity,
                   max_net_position, max_gross_position, max_risk_scale):
    pyplot.style.use('ggplot')
    backtest_result = backtest_portfolio(start_date, end_date, securities, prices_path, lookback_period=lookback_period,
                                         step_size=step_size, start_equity=start_equity,
                                         max_net_position=max_net_position,
                                         max_gross_position=max_gross_position,
                                         max_risk_scale=max_risk_scale)
    logging.info('fit quality: %s', fit_quality(backtest_result['equity'] - start_equity))
    backtest_result['equity'].plot()
    backtest_result['net_position'].plot()
    backtest_result['gross_position'].plot()
    pyplot.gca().get_yaxis().get_major_formatter().set_useOffset(False)
    backtest_result['factors'].plot(subplots=True)
    backtest_result['bollinger'].plot(subplots=False)
    pyplot.show()


def fit_quality(df):
    regr_df = df.reset_index()
    day_nanos = 24*60*60*1E9
    nanos = regr_df['date'] - regr_df['date'].min()
    df2 = pandas.DataFrame(data=[nanos.astype(int) / day_nanos, regr_df['equity']]).transpose()
    result = pandas.ols(y=df2['equity'], x=df2['date'], intercept=False)
    return {'p-value F-test': result.f_stat['p-value'], 'r-squared': result.r2, 'p-value x': result.p_value['x']}


def main(args):
    # TODO arg line
    prices_path = os.sep.join(['..', 'data', 'eod'])
    start_date = date(int(args.start_yyyymmdd[:4]), int(args.start_yyyymmdd[4:6]), int(args.start_yyyymmdd[6:8]))
    end_date = date(int(args.end_yyyymmdd[:4]), int(args.end_yyyymmdd[4:6]), int(args.end_yyyymmdd[6:8]))
    if args.display is not None:
        securities = args.display.split('/')
        chart_backtest(start_date, end_date, securities, prices_path, lookback_period=args.lookback_period,
                       step_size=args.step_size, start_equity=args.starting_equity,
                       max_net_position=args.max_net_position,
                       max_gross_position=args.max_gross_position,
                       max_risk_scale=args.max_risk_scale)

    elif args.display_portfolio is not None:
        pyplot.style.use('ggplot')
        backtest_results = dict()
        positions = pandas.DataFrame()
        holdings = pandas.DataFrame()
        fills = pandas.DataFrame()
        target_quantities = list()
        with open(args.display_portfolio) as portfolio_file:
            portfolios = [line.strip().split(',') for line in portfolio_file.readlines() if len(line.strip()) > 0]
            logging.info('loaded portfolios: %s' % portfolios)
            for lookback_period, portfolio in portfolios:
                securities = portfolio.split('/')
                backtest_result = backtest_portfolio(start_date, end_date, securities, prices_path, lookback_period=int(lookback_period),
                                                     step_size=args.step_size, start_equity=args.starting_equity,
                                                     max_net_position=args.max_net_position,
                                                     max_gross_position=args.max_gross_position,
                                                     max_risk_scale=args.max_risk_scale)
                positions = pandas.concat([positions, backtest_result['positions']])
                holdings = pandas.concat([holdings, backtest_result['holdings']])
                fills = pandas.concat([fills, backtest_result['fills']])
                if backtest_result['next_target_quantities'] is not None:
                    yahoo_codes = ['PCX/' + code for code in securities]
                    target_quantities += zip(yahoo_codes, backtest_result['next_target_quantities'])

                if 'equity' not in backtest_results:
                    backtest_results['equity'] = backtest_result['equity']

                else:
                    backtest_results['equity'] += backtest_result['equity']

        latest_holdings = holdings.pivot_table(index='date', columns='security', values='quantity', aggfunc=numpy.sum).tail(1).transpose()
        latest_holdings.columns = ['quantity']
        logging.info('stocks:\n%s' % latest_holdings)
        target_df = pandas.DataFrame(dict(target_quantities), index=[0]).transpose()
        target_df.columns=['target']
        logging.info('new target quantities:\n%s' % target_df)
        logging.info('trades:\n%s' % (target_df['target'] - latest_holdings['quantity']).dropna())

        equity = backtest_results['equity']
        benchmark = load_prices(prices_path, 'PCX', 'SPY')
        equity_df = pandas.concat([equity, benchmark['close adj']], axis=1).dropna()
        equity_df.columns = ['equity', 'benchmark']
        equity_df['benchmark'] = (equity_df['benchmark'].pct_change() + 1.).cumprod() * equity_df.head(1)['equity'].min()
        equity_df.plot()
        logging.info('fit quality: %s', fit_quality(equity - args.starting_equity))
        by_security_pos = positions.pivot_table(index='date', columns='security', values='position', aggfunc=numpy.sum)
        by_security_pos.plot()

        positions_aggregated_net = positions.groupby('date')['position'].sum()
        positions_aggregated_gross = positions.groupby('date')['position'].agg(lambda x: numpy.abs(x).sum())
        positions_aggregated = pandas.DataFrame(index=positions_aggregated_net.index,
                                                data=numpy.array([positions_aggregated_net, positions_aggregated_gross]).transpose(),
                                                columns=['net', 'gross'])
        positions_aggregated['margin_warning'] = equity / 0.4
        positions_aggregated.plot(subplots=False)

        days_interval = (equity.index[-1] - equity.index[0])
        starting_equity = equity.dropna().head(1)['equity'].values[0]
        ending_equity = equity.dropna().tail(1)['equity'].values[0]
        sharpe_ratio = equity.dropna().pct_change().mean() / equity.dropna().pct_change().std() * math.sqrt(250)
        logging.info('sharpe ratio: %.2f', sharpe_ratio)
        annualized_return = 100 * (numpy.power(ending_equity / starting_equity, 365 / days_interval.days) - 1)
        logging.info('annualized return: %.2f percent' % annualized_return)
        logging.info('fills:\n%s', fills.sort_values('date').set_index(['date', 'security']))
        pyplot.show()

    else:
        # backtest batch
        # TODO arg line
        portfolios_path = os.sep.join(['..', 'data', 'portfolios.csv'])
        with open(portfolios_path) as portfolios_file:
            portfolios = [line.strip().split(',') for line in portfolios_file.readlines()]
            results = list()
            for symbols in portfolios:
                backtest_result = backtest_portfolio(start_date, end_date, symbols, prices_path,
                                                     lookback_period=args.lookback_period,
                                            step_size=args.step_size, start_equity=args.starting_equity,
                                            max_net_position=args.max_net_position,
                                            max_gross_position=args.max_gross_position,
                                            max_risk_scale=args.max_risk_scale)
                backtest_data = fit_quality(backtest_result['equity'] - args.starting_equity)
                backtest_data.update(backtest_result['summary'])
                results.append(backtest_data)

            result_df = pandas.DataFrame(results).set_index('portfolio')
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
    parser.add_argument('--display', type=str, help='display portfolio made of comma-separated securities')
    parser.add_argument('--display-portfolio', type=str, help='display aggregated portfolio from specified file')
    parser.add_argument('--lookback-period', type=int, help='lookback period', default=200)
    parser.add_argument('--step-size', type=int, help='deviation unit measured in number of standard deviations', default=2)
    parser.add_argument('--starting-equity', type=float, help='deviation unit measured in number of standard deviations', default=8000)
    parser.add_argument('--max-net-position', type=float, help='max allowed net position for one step, measured as a fraction of equity', default=0.4)
    parser.add_argument('--max-gross-position', type=float, help='max allowed gross position by step, measured as a fraction of equity', default=2.)
    parser.add_argument('--max-risk-scale', type=int, help='max number of steps', default=3)
    args = parser.parse_args()
    main(args)
