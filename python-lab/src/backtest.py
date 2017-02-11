import argparse
import csv
import logging
import math

import numpy
import os
from datetime import date
from matplotlib import pyplot
import pandas
from statsmodels.formula.api import OLS
from statsmodels.tools import add_constant

from fls import FlexibleLeastSquare
from pnl import AverageCostProfitAndLoss


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
    #prices_df['date'] = prices_df['date'].apply(lambda x: pandas.to_datetime(x))
    #resampled_prices = prices_df.set_index('date').resample('B').last()
    #return resampled_prices


class PositionAdjuster(object):

    def __init__(self, securities, max_net_position, max_gross_position, max_risk_scale):
        self.securities = securities
        self.securities_index = {security: count for count, security in enumerate(securities)}
        self.trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self.current_quantities = [0.] * len(securities)
        self.max_net_position = max_net_position
        self.max_gross_position = max_gross_position
        self.max_risk_scale = max_risk_scale
        self.equity = 0.
        self.open_trades = list()
        self.closed_trades = list()
        self.current_risk_scale = 0.

    def move_to(self, timestamp, weights, prices, risk_scale):
        if abs(risk_scale) > self.max_risk_scale:
            logging.warning('risk scale %d exceeding max risk scale: capping to %d' % (risk_scale, self.max_risk_scale))
            if risk_scale > 0:
                risk_scale = self.max_risk_scale

            elif risk_scale < 0:
                risk_scale = -self.max_risk_scale

        if self.current_risk_scale == risk_scale:
            logging.warning('position already at specified risk scale: ignoring')
            return

        logging.debug('moving to position: %d at %s' % (risk_scale, timestamp.strftime('%Y-%m-%d')))
        strategy_equity = self.get_nav(prices)
        scaling = 0
        if risk_scale != 0:
            scaling_gross = self.equity * self.max_gross_position / numpy.max(numpy.abs(weights))
            scaling_net = self.equity * self.max_net_position / numpy.abs(numpy.sum(weights))
            scaling = min(scaling_net, scaling_gross)

        for count, weight_price in enumerate(zip(weights, prices)):
            weight, price = weight_price
            target_position = scaling * risk_scale * weight
            target_quantity = round(target_position / price)
            fill_qty = target_quantity - self.current_quantities[count]
            trades_tracker = self.trades_tracker[self.securities[count]]
            trades_tracker.add_fill(fill_qty, price)
            self.current_quantities[count] = target_quantity

        logging.debug('current positions: %s' % str(self.get_positions(prices)))
        trades_count = int(abs(risk_scale) - abs(self.current_risk_scale))
        if trades_count > 0:
            logging.debug('opening %d trade(s)' % trades_count)
            for trade_count in range(trades_count):
                self.open_trades.append({'open': timestamp,
                                         'risk_level': risk_scale,
                                         'equity_start': strategy_equity,
                                         'equity_end': strategy_equity,
                                         'pnl': 0.
                                         })

        else:
            logging.debug('closing %d trade(s)' % abs(trades_count))
            for trade_count in range(abs(trades_count)):
                trade_result = self.open_trades.pop()
                trade_result['close'] = timestamp
                self.closed_trades.append(trade_result)

            # when multiple risk levels attributes whole pnl to last one
            self.closed_trades[-1]['equity_end'] = self.get_nav(prices)
            self.closed_trades[-1]['pnl'] = self.closed_trades[-1]['equity_end'] - self.closed_trades[-1]['equity_start']

        self.current_risk_scale = risk_scale

    def get_nav(self, prices):
        total_pnl = 0.
        for security in self.trades_tracker:
            pnl_calc = self.trades_tracker[security]
            price = prices[self.securities_index[security]]
            total_pnl += pnl_calc.get_total_pnl(price)

        return total_pnl

    def get_positions(self, prices):
        return numpy.array(self.current_quantities) * prices

    def get_position_securities(self, prices):
        return dict(zip(self.securities, [position for position in self.get_positions(prices)]))

    def update_equity(self, equity):
        self.equity = equity


class Strategy(object):

    def __init__(self, securities, step_size, start_equity, max_net_position, max_gross_position, max_risk_scale):
        """

        :param securities:
        :param step_size:
        :param start_equity:
        :param max_net_position: measured in fraction of equity
        :param max_gross_position: measured in fraction of equity
        :param max_risk_scale: max number of deviations steps allowed
        """
        self.current_level = 0.
        self.signal_zone = 0
        self.start_equity = start_equity
        self.equity_history = list()
        self.net_position_history = list()
        self.gross_position_history = list()
        self.positions_history = list()
        self.position_adjuster = PositionAdjuster(securities, max_net_position, max_gross_position, max_risk_scale)
        self.position_adjuster.update_equity(start_equity)
        self.deviation = 0.
        self._step_size = step_size

    def update_state(self, timestamp, signal, deviation, weights, traded_prices):
        if deviation == 0.:
            return

        self.deviation = deviation * self._step_size

        if signal >= self.level_sup():
            self.signal_zone += 1
            self.position_adjuster.move_to(timestamp, weights, traded_prices, self.signal_zone)

        if signal <= self.level_inf():
            self.signal_zone -= 1
            self.position_adjuster.move_to(timestamp, weights, traded_prices, self.signal_zone)

        equity = self.position_adjuster.get_nav(traded_prices) + self.start_equity
        self.position_adjuster.update_equity(equity)
        self.equity_history.append({'date': timestamp, 'pnl': equity})
        positions = self.position_adjuster.get_positions(traded_prices)
        net_positions = positions.sum()
        gross_positions = numpy.abs(positions).sum()
        self.net_position_history.append({'date': timestamp, 'net_position': net_positions})
        self.gross_position_history.append({'date': timestamp, 'gross_position': gross_positions})
        positions_data = {'date': timestamp, 'strategy': self.get_name()}
        positions = self.position_adjuster.get_position_securities(traded_prices)
        for security in positions:
            positions_data['position'] = positions[security]
            positions_data['security'] = security

        self.positions_history.append(positions_data)

    def get_name(self):
        return ','.join(self.position_adjuster.securities)

    def level_inf(self):
        return (self.signal_zone - 1) * self.deviation

    def level_sup(self):
        return (self.signal_zone + 1) * self.deviation

    def get_equity(self):
        return pandas.DataFrame(self.equity_history).set_index('date')

    def get_positions(self):
        return pandas.DataFrame(self.positions_history)

    def get_net_position(self):
        return pandas.DataFrame(self.net_position_history).set_index('date')

    def get_gross_position(self):
        return pandas.DataFrame(self.gross_position_history).set_index('date')

    def get_closed_trades(self):
        return pandas.DataFrame(self.position_adjuster.closed_trades)

    def get_open_trades(self):
        return pandas.DataFrame(self.position_adjuster.open_trades)

    def get_sharpe_ratio(self):
        mean_return = self.get_equity().pct_change().mean()
        std_return = self.get_equity().pct_change().std()
        value = mean_return / std_return * math.sqrt(250)
        return value['pnl']

    def get_drawdown(self):
        cum_returns = (1. + self.get_equity().pct_change()).cumprod()
        return 1. - cum_returns.div(cum_returns.cummax())


class RegressionModelFLS(object):

    def __init__(self, securities, delta, with_constant_term=True):
        self.with_constant_term = with_constant_term
        size = len(securities) - 1
        if self.with_constant_term:
            size += 1

        initial_state_mean = numpy.zeros(size)
        initial_state_covariance = numpy.ones((size, size))
        observation_covariance = 5E-5
        trans_cov = delta / (1. - delta) * numpy.eye(size)

        self.result = None
        self.fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)

    def compute_regression(self, y_value, x_values):
        independent_values = x_values
        if self.with_constant_term:
            independent_values += [1.]

        self.result = self.fls.estimate(y_value, independent_values)

    def get_residual_error(self):
        return math.sqrt(self.result.var_output_error)

    def get_factors(self):
        return self.result.beta

    def get_estimate(self):
        return self.result.estimated_output

    def get_weights(self):
        weights = self.get_factors()
        if self.with_constant_term:
            weights = weights[:-1]

        return numpy.array([-1.] + weights)

    def get_residual(self):
        return self.result.error


class RegressionModelOLS(object):

    def __init__(self, securities, with_constant_term=True, lookback_period=200):
        self._lookback = lookback_period
        self.current_x = None
        self.current_y = None
        self._with_constant_term= with_constant_term
        self._counter = 0
        self._y_values = list()
        self._x_values = [list() for item in securities[1:]]
        self.securities = securities
        self.result = None
        self.ols = None

    def compute_regression(self, y_value, x_values):
        self._counter += 1
        self._y_values.append(y_value)
        if self._counter > self._lookback:
            self._y_values.pop(0)
            for target_lists in self._x_values:
                target_lists.pop(0)

        for target_list, new_item in zip(self._x_values, x_values):
            target_list.append(new_item)

        dependent = pandas.DataFrame({self.securities[0]: self._y_values})
        independent = pandas.DataFrame({key: self._x_values[count] for count, key in enumerate(self.securities[1:])})
        if self._with_constant_term:
            self.ols = OLS(dependent, add_constant(independent))

        else:
            self.ols = OLS(dependent, independent)

        self.result = self.ols.fit()
        independent_values = x_values
        if self._with_constant_term:
            independent_values += [1.]

        self.current_x = independent_values
        self.current_y = y_value

    def get_residual_error(self):
        """
        Standard error of estimates.
        :return:
        """
        return math.sqrt(self.result.mse_resid)

    def get_factors(self):
        if self._with_constant_term:
            return self.result.params[self.securities[1:] + ['const']]

        else:
            return self.result.params[self.securities[1:]]

    def get_estimate(self):
        if self._with_constant_term:
            return self.result.params[self.securities[1:] + ['const']].dot(self.current_x)

        else:
            return self.result.params[self.securities[1:]].dot(self.current_x)

    def get_weights(self):
        return numpy.array([-1.] + self.result.params[self.securities[1:]].tolist())

    def get_residual(self):
        return self.current_y - self.get_estimate()


def process_strategy(securities, signal_data, regression, warmup_period, prices_by_security,
                     step_size, start_equity, max_net_position, max_gross_position, max_risk_scale):
    trader_engine = Strategy(securities=securities, step_size=step_size, start_equity=start_equity,
                             max_net_position=max_net_position,
                             max_gross_position=max_gross_position,
                             max_risk_scale=max_risk_scale)

    chart_bollinger = list()
    chart_beta = list()
    chart_regression = list()
    for count_day, rows in enumerate(zip(signal_data.iterrows(), signal_data.shift(-1).iterrows())):
        row, row_next = rows
        row_count, price_data = row
        row_count_next, price_data_next = row_next
        timestamp = price_data['date']
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        regression.compute_regression(dependent_price, independent_prices.tolist())
        dev_fls = regression.get_residual_error()
        level_inf = trader_engine.level_inf()
        level_sup = trader_engine.level_sup()
        next_date = price_data_next['date']
        signal = 0.
        if count_day > warmup_period and not numpy.isnan(price_data_next[securities[0]]):
            weights = regression.get_weights()
            signal = regression.get_residual()
            traded_prices = list()
            for security in securities:
                prices = prices_by_security[security][prices_by_security[security].index == next_date]
                if len(prices) == 0:
                    logging.error('no data as of %s' % (next_date))
                    raise RuntimeError('no data as of %s' % (next_date))

                traded_prices.append(prices['open'].values[0])

            trader_engine.update_state(timestamp, signal, dev_fls, weights, traded_prices)

        signal_data = {
            'date': timestamp,
            'level_inf': level_inf,
            'level_sup': level_sup,
            'signal_fls': signal
        }

        chart_bollinger.append(signal_data)

        beta_data = dict()
        for count_factor, weight in enumerate(regression.get_factors()):
            beta_data['beta%d' % count_factor] = weight

        beta_data['date'] = timestamp
        chart_beta.append(beta_data)

        regression_data = {'date': timestamp,
                           'y*': regression.get_estimate(),
                           'y': dependent_price,
                           'portfolio': regression.get_estimate() - regression.get_factors()[-1]
                           }
        chart_regression.append(regression_data)

    closed_trades = trader_engine.get_closed_trades()
    mean_trade = closed_trades['pnl'].mean()
    worst_trade = closed_trades['pnl'].min()
    count_trades = closed_trades['pnl'].count()
    max_drawdown = trader_engine.get_drawdown().max()['pnl']
    final_equity = trader_engine.get_equity()['pnl'][-1]
    summary = {
        'sharpe_ratio': trader_engine.get_sharpe_ratio(),
        'average_trade': mean_trade,
        'worst_trade': worst_trade,
        'count_trades': count_trades,
        'max_drawdown_pct': max_drawdown,
        'final_equity': final_equity
    }
    logging.info('result: %s' % str(summary))
    result = {
        'summary': summary,
        'bollinger': pandas.DataFrame(chart_bollinger).set_index('date'),
        'factors': pandas.DataFrame(chart_beta).set_index('date'),
        'regression': chart_regression,
        'equity': trader_engine.get_equity(),
        'net_position': trader_engine.get_net_position(),
        'gross_position': trader_engine.get_gross_position(),
        'positions': trader_engine.get_positions(),
    }
    return result


def run_backtest(symbols, prices_path, lookback_period,
                 step_size, start_equity, max_net_position, max_gross_position,
                 max_risk_scale):
    securities = ['PCX/' + symbol for symbol in symbols]
    prices_by_security = dict()
    close_prices = pandas.DataFrame()
    max_start_date = date(2013, 1, 1)
    min_end_date = None
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
    signal_data = close_prices[(close_prices['date'] <= min_end_date) & (close_prices['date'] >= max_start_date)]

    for security in securities:
        prices_by_security[security] = prices_by_security[security][prices_by_security[security] >= max_start_date]

    warmup_period = 10

    #delta = 5E-6
    #regression0 = RegressionModelFLS(securities, delta, with_constant_term=False)
    #process_strategy(securities, signal_data, regression0, warmup_period, prices_by_security)
    regression10 = RegressionModelOLS(securities, with_constant_term=False, lookback_period=lookback_period)
    backtest_data = process_strategy(securities, signal_data, regression10, warmup_period, prices_by_security,
                                     step_size=step_size, start_equity=start_equity,
                                     max_net_position=max_net_position,
                                     max_gross_position=max_gross_position,
                                     max_risk_scale=max_risk_scale)
    backtest_summary = backtest_data['summary']
    backtest_summary['portfolio'] = '/'.join(symbols)
    return backtest_data


def chart_backtest(securities, prices_path, lookback_period,
                   step_size, start_equity,
                   max_net_position, max_gross_position, max_risk_scale):
    pyplot.style.use('ggplot')
    backtest_result = run_backtest(securities, prices_path, lookback_period=lookback_period,
                                   step_size=step_size, start_equity=start_equity,
                                   max_net_position=max_net_position,
                                   max_gross_position=max_gross_position,
                                   max_risk_scale=max_risk_scale)
    backtest_result['equity'].plot()
    backtest_result['net_position'].plot()
    backtest_result['gross_position'].plot()
    pyplot.gca().get_yaxis().get_major_formatter().set_useOffset(False)
    backtest_result['factors'].plot(subplots=True)
    backtest_result['bollinger'].plot(subplots=False)
    pyplot.show()


def main(args):
    prices_path = os.sep.join(['..', '..', 'data', 'eod'])
    if args.display is not None:
        securities = args.display.split('/')
        chart_backtest(securities, prices_path, lookback_period=args.lookback_period,
                       step_size=args.step_size, start_equity=args.starting_equity,
                       max_net_position=args.max_net_position,
                       max_gross_position=args.max_gross_position,
                       max_risk_scale=args.max_risk_scale)

    elif args.display_portfolio is not None:
        pyplot.style.use('ggplot')
        backtest_results = dict()
        positions = pandas.DataFrame()
        with open(args.display_portfolio) as portfolio_file:
            portfolios = [line.strip().split(',') for line in portfolio_file.readlines() if len(line.strip()) > 0]
            logging.info('loaded portfolios: %s' % portfolios)
            for lookback_period, portfolio in portfolios:
                securities = portfolio.split('/')
                backtest_result = run_backtest(securities, prices_path, lookback_period=int(lookback_period),
                                   step_size=args.step_size, start_equity=args.starting_equity,
                                   max_net_position=args.max_net_position,
                                   max_gross_position=args.max_gross_position,
                                   max_risk_scale=args.max_risk_scale)
                positions = pandas.concat([positions, backtest_result['positions']])
                if 'equity' not in backtest_results:
                    backtest_results['equity'] = backtest_result['equity']

                else:
                    backtest_results['equity'] += backtest_result['equity']

        equity = backtest_results['equity']
        equity.plot()
        days_interval = (equity.index[-1] - equity.index[0])
        starting_equity = equity.dropna().head(1)['pnl'].values[0]
        ending_equity = equity.dropna().tail(1)['pnl'].values[0]
        annualized_return = 100 * (numpy.power(ending_equity / starting_equity, 365 / days_interval.days) - 1)
        logging.info('annualized return: %.2f percent' % annualized_return)
        pyplot.show()

    else:
        portfolios_path = os.sep.join(['..', '..', 'data', 'portfolios.csv'])
        with open(portfolios_path) as portfolios_file:
            portfolios = [line.strip().split(',') for line in portfolios_file.readlines()]
            results = list()
            for symbols in portfolios:
                result = run_backtest(symbols, prices_path, lookback_period=args.lookback_period,
                                      step_size=args.step_size, start_equity=args.starting_equity,
                                      max_net_position=args.max_net_position,
                                      max_gross_position=args.max_gross_position,
                                      max_risk_scale=args.max_risk_scale)

                results.append(result['summary'])

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
    parser.add_argument('--display', type=str, help='display portfolio made of comma-separated securities')
    parser.add_argument('--display-portfolio', type=str, help='display aggregated portfolio from specified file')
    parser.add_argument('--lookback-period', type=int, help='lookback period', default=200)
    parser.add_argument('--step-size', type=int, help='deviation unit measured in number of standard deviations', default=2)
    parser.add_argument('--starting-equity', type=float, help='deviation unit measured in number of standard deviations', default=20000)
    parser.add_argument('--max-net-position', type=float, help='max allowed net position for one step, measured as a fraction of equity', default=0.4)
    parser.add_argument('--max-gross-position', type=float, help='max allowed gross position for one step, measured as a fraction of equity', default=1.2)
    parser.add_argument('--max-risk-scale', type=int, help='max number of steps', default=3)
    args = parser.parse_args()
    main(args)

#securities = ['PCX/' + symbol for symbol in ['AMLP','PFF','PGX']]
#securities = ['PCX/' + symbol for symbol in ['BKLN', 'HYG', 'JNK']]
#securities = ['PCX/' + symbol for symbol in ['IYR', 'PFF']]
#securities = ['PCX/' + symbol for symbol in ['VNQ', 'XLU']]
#securities = ['PCX/' + symbol for symbol in ['PFF', 'XLU']]
#securities = ['PCX/' + symbol for symbol in ['PFF', 'XLU', 'VNQ']]
#securities = ['PCX/' + symbol for symbol in ['IYR', 'XLU']]
#securities = ['PCX/' + symbol for symbol in ['GDX', 'SLV']]
#securities = ['PCX/' + symbol for symbol in ['IYR', 'LQD']]
#securities = ['PCX/' + symbol for symbol in ['EWC', 'EWY']]
#securities = ['PCX/' + symbol for symbol in ['BND', 'EMB']]
#securities = ['PCX/' + symbol for symbol in ['EMB', 'LQD']]
#securities = ['PCX/' + symbol for symbol in ['AGG', 'PGX']]
#securities = ['PCX/' + symbol for symbol in ['AGG', 'PFF']]
#securities = ['PCX/' + symbol for symbol in ['IAU', 'SLV']]
#securities = ['PCX/' + symbol for symbol in ['AGG', 'IYR']]
#securities = ['PCX/' + symbol for symbol in ['UNG', 'XOP']]
#securities = ['PCX/' + symbol for symbol in ['AMLP', 'VWO']]
#securities = ['PCX/' + symbol for symbol in ['EWZ', 'XME']]
#securities = ['PCX/' + symbol for symbol in ['USMV', 'XLU']]
#securities = ['PCX/' + symbol for symbol in ['AGG', 'VNQ']]
#securities = ['PCX/' + symbol for symbol in ['AGG', 'EMB']]
#securities = ['PCX/' + symbol for symbol in ['AMLP', 'EEM']]
#securities = ['PCX/' + symbol for symbol in ['LQD', 'VNQ']]
#securities = ['PCX/' + symbol for symbol in ['EWC', 'VWO']]
#securities = ['PCX/' + symbol for symbol in ['EWH', 'XLB']]
#securities = ['PCX/' + symbol for symbol in ['USMV', 'XLP']]
#securities = ['PCX/' + symbol for symbol in ['LQD', 'PFF']]
#securities = ['PCX/' + symbol for symbol in ['EFA', 'ITB', 'XOP']]
#

