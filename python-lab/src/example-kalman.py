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


class Trader(object):

    def __init__(self, scaling, securities):
        self.scaling = scaling
        self.securities = securities
        self.positions = numpy.zeros(len(securities))
        self.cost_value = 0.
        self.trade_pnl = list()
        self.daily_nav = list()
        self.cash_position = 0.

    def position_enter(self, timestamp, target_level, weights, close_prices):
            self._position_close(timestamp, close_prices)
            positions = weights * target_level * self.scaling
            self.positions = positions.round(0)
            self.cost_value = self.positions.dot(numpy.array(close_prices))
            position_values = self.positions * numpy.array(close_prices)
            self.cash_position -= position_values.sum()
            for sec, pos, px in zip(self.securities, self.positions.tolist(), close_prices):
                logging.info('entering position %s: %s at %s' % (sec, pos, px))

            logging.info('%s: entering total position value=%f' % (timestamp, self.cost_value))

    def _position_close(self, timestamp, close_prices):
        value = self.positions.dot(numpy.array(close_prices))
        position_values = self.positions * numpy.array(close_prices)
        self.cash_position += position_values.sum()
        for sec, pos, px in zip(self.securities, self.positions.tolist(), close_prices):
            logging.info('solding out position %s: %s at %s' % (sec, pos, px))

        logging.info('%s: solding out positions %s, value=%s' % (timestamp, self.positions, value))
        pnl = value - self.cost_value
        logging.info('-----> P&L: %f' % pnl)
        self.trade_pnl.append({'date': timestamp, 'pnl': pnl})
        self.positions = numpy.zeros(len(close_prices))

    def df_trade_pnl(self):
        df_pnl = None
        if len(self.trade_pnl) > 0:
            df_pnl = pandas.DataFrame(self.trade_pnl)
            df_pnl.set_index('date', inplace=True)

        return df_pnl

    def df_daily_nav(self):
        df_nav = pandas.DataFrame(self.daily_nav)
        df_nav.set_index('date', inplace=True)
        return df_nav

    def evaluate(self, timestamp, close_prices):
        value = self.positions.dot(numpy.array(close_prices))
        self.daily_nav.append({'date': timestamp, 'nav': self.cash_position + value})


class RegressionModelFLS(object):

    def __init__(self, initial_state_mean, initial_state_covariance, observation_covariance, trans_cov):
        self.result = None
        self.fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)

    def compute_regression(self, y_value, x_values):
        self.result = self.fls.estimate(y_value, x_values)

    def get_residual_error(self):
        return math.sqrt(self.result.var_output_error) / 20.

    def get_factors(self):
        return self.result.beta

    def get_weights(self):
        return numpy.array([-1.] + self.get_factors()[:-1])

    def get_residual(self):
        return self.result.error


class RegressionModelOLS(object):

    def __init__(self, securities, y_values, x_values):
        self.current_x = None
        self.current_y = None
        self.ols = OLS(y_values, add_constant(x_values))
        self.result = self.ols.fit()
        self.securities = securities

    def compute_regression(self, y_value, x_values):
        self.current_x = x_values
        self.current_y = y_value

    def get_residual_error(self):
        return self.result.mse_resid * 20.

    def get_factors(self):
        return self.result.params[self.securities[1:] + ['const']]

    def get_weights(self):
        return numpy.array([-1.] + self.result.params[self.securities[1:]].tolist())

    def get_residual(self):
        return self.current_y - self.result.params[self.securities[1:] + ['const']].dot(self.current_x)


def process_with_regression(securities, prices, regression, warmup_period):
    chart_bollinger = list()
    chart_beta = list()
    signal_fls = 0.
    trigger_up = numpy.nan
    trigger_down = numpy.nan
    enter_next_fls = None
    trader_fls = Trader(scaling=100., securities=securities)
    for count, row in enumerate(prices.iterrows()):
        row_count, price_data = row
        timestamp = price_data['date']
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        regression.compute_regression(dependent_price, independent_prices.tolist() + [1.])
        dev_fls = regression.get_residual_error()
        if numpy.isnan(trigger_down):
            trigger_down = -dev_fls

        if numpy.isnan(trigger_up):
            trigger_up = dev_fls

        weights_fls = regression.get_weights()
        close_prices = [dependent_price] + independent_prices.tolist()

        if count > warmup_period:
            signal_fls = regression.get_residual()
            if signal_fls > trigger_up:
                enter_next_fls = int(regression.get_residual() / dev_fls) * -1., weights_fls
                trigger_up += dev_fls
                trigger_down += dev_fls

            if signal_fls < trigger_down:
                enter_next_fls = int(regression.get_residual() / dev_fls) * 1., weights_fls
                trigger_up -= dev_fls
                trigger_down -= dev_fls

            if enter_next_fls is not None:
                target_level, target_weights = enter_next_fls
                trader_fls.position_enter(timestamp, target_level, target_weights, close_prices)

        trader_fls.evaluate(timestamp, close_prices)

        samples = {'limit_inf': trigger_down, 'limit_sup': trigger_up, 'signal_fls': signal_fls}
        chart_bollinger.append(samples)

        beta_data = dict()
        for count, weight in enumerate(regression.get_factors()):
            beta_data['beta%d' % count] = weight

        chart_beta.append(beta_data)

    trader_fls.df_daily_nav().plot()
    pandas.DataFrame(chart_beta).plot(subplots=True)
    pandas.DataFrame(chart_bollinger).plot(subplots=False)


def main(args):
    pyplot.style.use('ggplot')
    prices_path = os.sep.join(['..', 'resources','eod.pkl'])
    all_prices = pandas.read_pickle(prices_path)

    #securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
    #securities = ['PCX/' + symbol for symbol in ['SPY','VOO']]
    securities = ['PCX/' + symbol for symbol in ['IAU','GLD']]
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

    delta = 5E-6

    initial_state_mean = numpy.zeros(len(securities))
    initial_state_covariance = numpy.ones((len(securities), len(securities)))
    observation_covariance = 5E-5
    trans_cov = delta / (1. - delta) * numpy.eye(len(securities))

    warmup_period = 10
    regress = OLS(in_sample_prices[securities[0]], add_constant(in_sample_prices[securities[1:]])).fit()
    logging.info('regression results: %s' % str(regress.params))
    regression = RegressionModelFLS(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
    regression2 = RegressionModelOLS(securities, in_sample_prices[securities[0]], in_sample_prices[securities[1:]])
    process_with_regression(securities, in_sample_prices, regression, warmup_period)
    process_with_regression(securities, in_sample_prices, regression2, warmup_period)
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
