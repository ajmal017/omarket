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


class PositionAdjuster(object):

    def __init__(self, securities, scaling):
        self.securities = securities
        self.securities_index = {security: count for count, security in enumerate(securities)}
        self.trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self.current_quantities = [0.] * len(securities)
        self.scaling = scaling

    def move_to(self, timestamp, weights, prices, target_position):
        logging.info('moving to position: %d at %s' % (target_position, timestamp.strftime('%Y-%m-%d')))
        for count, weight_price in enumerate(zip(weights, prices)):
            weight, price = weight_price
            target_weight = target_position * weight
            target_quantity = round(self.scaling * target_weight / price)
            fill_qty = target_quantity - self.current_quantities[count]
            self.trades_tracker[self.securities[count]].add_fill(fill_qty, price)
            self.current_quantities[count] = target_quantity

    def get_nav(self, close_prices):
        total_pnl = 0.
        for security in self.trades_tracker:
            pnl_calc = self.trades_tracker[security]
            price = close_prices[self.securities_index[security]]
            total_pnl += pnl_calc.get_total_pnl(price)

        return total_pnl


class Trader(object):

    def __init__(self, scaling, securities, step_size):
        self.current_level = 0.
        self.signal_zone = 0
        self.position_adjuster = PositionAdjuster(securities, scaling)
        self.equity = list()
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

        self.equity.append({'date': timestamp, 'pnl': self.position_adjuster.get_nav(traded_prices)})

    def level_inf(self):
        return (self.signal_zone - 1) * self.deviation

    def level_sup(self):
        return (self.signal_zone + 1) * self.deviation

    def get_equity(self):
        return pandas.DataFrame(self.equity).set_index('date')


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


def process_with_regression(securities, signal_data, regression, warmup_period, prices_by_security):
    chart_bollinger = list()
    chart_beta = list()
    chart_regression = list()
    signal = 0.
    trader_engine = Trader(scaling=10000., securities=securities, step_size=2)
    for count, rows in enumerate(zip(signal_data.iterrows(), signal_data.shift(-1).iterrows())):
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
        if count > warmup_period and not numpy.isnan(price_data_next[securities[0]]):
            weights = regression.get_weights()
            signal = regression.get_residual()
            traded_prices = list()
            for security in securities:
                prices = prices_by_security[security][prices_by_security[security].index == next_date]
                if len(prices) == 0:
                    logging.error('no data as of %s' % (next_date))

                traded_prices.append(prices['open'].values[0])

            trader_engine.update_state(timestamp, signal, dev_fls, weights, traded_prices)

        signal_data = {
            'date': timestamp,
            'level_inf': level_inf,
            'level_sup': level_sup,
            'signal_fls': signal}
        chart_bollinger.append(signal_data)

        beta_data = dict()
        for count, weight in enumerate(regression.get_factors()):
            beta_data['beta%d' % count] = weight

        beta_data['date'] = timestamp
        chart_beta.append(beta_data)

        regression_data = {'date': timestamp, 'y*': regression.get_estimate(), 'y': dependent_price, 'portfolio': regression.get_estimate() - regression.get_factors()[-1]}
        chart_regression.append(regression_data)

    trader_engine.get_equity().plot()
    pandas.DataFrame(chart_beta).set_index('date').plot(subplots=True)
    pandas.DataFrame(chart_bollinger).set_index('date').plot(subplots=False)
    #pandas.DataFrame(chart_regression).set_index('date').plot(subplots=False)


def main(args):
    pyplot.style.use('ggplot')

    #securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','VOO']]
    #securities = ['PCX/' + symbol for symbol in ['IAU','GDX']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','IWM']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','UUP','VOO']]
    #securities = ['PCX/' + symbol for symbol in ['VWO','XLB','XLI']]
    #securities = ['PCX/' + symbol for symbol in ['EFA','SCHF','VEU']]
    #securities = ['PCX/' + symbol for symbol in ['EEM','XLB','XLI']]
    #securities = ['PCX/' + symbol for symbol in ['VEA','VEU','VWO']]
    #securities = ['PCX/' + symbol for symbol in ['IWD','XLE','XOP']]
    securities = ['PCX/' + symbol for symbol in ['USMV','XLU','XLY']]
    #securities = ['PCX/' + symbol for symbol in ['GDX','SLV','XLU']]
    #securities = ['PCX/' + symbol for symbol in ['GDX','IAU','KRE']]
    #securities = ['PCX/' + symbol for symbol in ['AGG','BND','PGX']]
    #securities = ['PCX/' + symbol for symbol in ['AGG','IAU','PFF']]

    prices_path = os.sep.join(['..', '..', 'data', 'eod'])
    prices_by_security = dict()
    prices = pandas.DataFrame()
    for security in securities:
        exchange, security_code = security.split('/')
        prices_df = load_prices(prices_path, exchange, security_code)
        prices_by_security[security] = prices_df
        prices[security] = prices_df['close adj']

    prices.reset_index(inplace=True)
    signal_data = prices[prices['date'] < date(2015, 1, 1)]

    warmup_period = 10

    delta = 5E-6
    #regression0 = RegressionModelFLS(securities, delta, with_constant_term=False)
    #regression1 = RegressionModelFLS(securities, 0.5, with_constant_term=False)
    #regression2 = RegressionModelFLS(securities, 0.9, with_constant_term=False)
    regression10 = RegressionModelOLS(securities, with_constant_term=False)
    #process_with_regression(securities, in_sample_prices, regression0, warmup_period)
    #process_with_regression(securities, in_sample_prices, regression1, warmup_period)
    #process_with_regression(securities, in_sample_prices, regression2, warmup_period)
    process_with_regression(securities, signal_data, regression10, warmup_period, prices_by_security)
    pyplot.show()


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
    args = parser.parse_args()
    main(args)

