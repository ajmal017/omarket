import argparse
from datetime import date
import logging
import math
import numpy
import os
from matplotlib import pyplot
import pandas
from statsmodels.formula.api import OLS
from statsmodels.tools import add_constant

from discretize import HysteresisDiscretize, discretize
from fls import FlexibleLeastSquare


class PositionAdjuster(object):
    def __init__(self, securities):
        self.securities = securities

    def adjust(self, timestamp, weights, close_prices, target_position):
        pass


class Trader(object):

    def __init__(self, scaling, securities):
        self.current_level = 0.
        self.target_position = 0
        self.trigger_level_inf = None
        self.trigger_level_sup = None
        self.position_adjuster = PositionAdjuster(securities)

    def update_state(self, timestamp, signal, deviation, weights, close_prices):
        if deviation == 0.:
            return

        if self.trigger_level_inf is None:
            self.trigger_level_inf = -deviation

        if self.trigger_level_sup is None:
            self.trigger_level_sup = deviation

        if signal >= self.trigger_level_sup:
            self.current_level = self.trigger_level_sup
            self.target_position += 1
            self.position_adjuster.adjust(timestamp, weights, close_prices, self.target_position)
            self.trigger_level_inf = (self.target_position - 1) * deviation
            self.trigger_level_sup = (self.target_position + 1) * deviation

        if signal <= self.trigger_level_inf:
            self.current_level = self.trigger_level_inf
            self.target_position -= 1
            self.position_adjuster.adjust(timestamp, weights, close_prices, self.target_position)
            self.trigger_level_inf = (self.target_position - 1) * deviation
            self.trigger_level_sup = (self.target_position + 1) * deviation

    def level_current(self):
        return self.current_level

    def level_inf(self):
        return self.trigger_level_inf

    def level_sup(self):
        return self.trigger_level_sup


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


def process_with_regression(securities, prices, regression, warmup_period):
    chart_bollinger = list()
    chart_beta = list()
    chart_regression = list()
    signal_fls = 0.
    trader_fls = Trader(scaling=100., securities=securities)

    for count, row in enumerate(prices.iterrows()):
        row_count, price_data = row
        timestamp = price_data['date']
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        regression.compute_regression(dependent_price, independent_prices.tolist())
        dev_fls = regression.get_residual_error()
        deviation = 2. * dev_fls
        #deviation = 3.  # DEBUG

        close_prices = [dependent_price] + independent_prices.tolist()
        if count > warmup_period:
            weights_fls = regression.get_weights()
            signal_fls = regression.get_residual()
            trader_fls.update_state(timestamp, signal_fls, deviation, weights_fls, close_prices)

        signal_data = {
            'date': timestamp,
            'limit_inf': -deviation,
            'limit_sup': deviation,
            'level_inf': trader_fls.level_inf(),
            'level_sup': trader_fls.level_sup(),
            'position': trader_fls.target_position * 5,
            'signal_fls': signal_fls}
        chart_bollinger.append(signal_data)

        beta_data = dict()
        for count, weight in enumerate(regression.get_factors()):
            beta_data['beta%d' % count] = weight

        beta_data['date'] = timestamp
        chart_beta.append(beta_data)

        regression_data = {'date': timestamp, 'y*': regression.get_estimate(), 'y': dependent_price, 'portfolio': regression.get_estimate() - regression.get_factors()[-1]}
        chart_regression.append(regression_data)

    #trader_fls.df_daily_nav().plot()
    pandas.DataFrame(chart_beta).set_index('date').plot(subplots=True)
    pandas.DataFrame(chart_bollinger).set_index('date').plot(subplots=False)
    #pandas.DataFrame(chart_regression).set_index('date').plot(subplots=False)


def main(args):
    pyplot.style.use('ggplot')
    prices_path = os.sep.join(['..', 'resources','eod.pkl'])
    all_prices = pandas.read_pickle(prices_path)

    #securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','VOO']]
    #securities = ['PCX/' + symbol for symbol in ['IAU','GDX']]
    securities = ['PCX/' + symbol for symbol in ['SPY','IWM']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','UUP','VOO']]
    #securities = ['PCX/' + symbol for symbol in ['VWO','XLB','XLI']]
    #securities = ['PCX/' + symbol for symbol in ['EFA','SCHF','VEU']]
    #securities = ['PCX/' + symbol for symbol in ['EEM','XLB','XLI']]
    #securities = ['PCX/' + symbol for symbol in ['VEA','VEU','VWO']]
    #securities = ['PCX/' + symbol for symbol in ['IWD','XLE','XOP']]
    #securities = ['PCX/' + symbol for symbol in ['USMV','XLU','XLY']]
    #securities = ['PCX/' + symbol for symbol in ['GDX','SLV','XLU']]
    #securities = ['PCX/' + symbol for symbol in ['GDX','IAU','KRE']]
    #securities = ['PCX/' + symbol for symbol in ['AGG','BND','PGX']]
    #securities = ['PCX/' + symbol for symbol in ['AGG','IAU','PFF']]


    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices  # [prices['date'] < date(2015, 1, 1)]

    warmup_period = 10

    delta = 5E-6
    #regression0 = RegressionModelFLS(securities, delta, with_constant_term=False)
    #regression1 = RegressionModelFLS(securities, 0.5, with_constant_term=False)
    #regression2 = RegressionModelFLS(securities, 0.9, with_constant_term=False)
    regression10 = RegressionModelOLS(securities, with_constant_term=False)
    #process_with_regression(securities, in_sample_prices, regression0, warmup_period)
    #process_with_regression(securities, in_sample_prices, regression1, warmup_period)
    #process_with_regression(securities, in_sample_prices, regression2, warmup_period)
    process_with_regression(securities, in_sample_prices, regression10, warmup_period)
    pyplot.show()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('example-kalman.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='Running Kalman Algo.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    args = parser.parse_args()
    main(args)
