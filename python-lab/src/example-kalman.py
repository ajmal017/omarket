import argparse
from datetime import date
import logging
import math
import numpy
import os
from matplotlib import pyplot
import pandas

from discretize import HysteresisDiscretize, discretize
from fls import FlexibleLeastSquare


def position_enter(timestamp, position_sign, weights, close_prices):
    scaling = 50.
    positions = weights * position_sign * scaling
    positions = positions.round(0)
    value = positions.dot(numpy.array(close_prices))
    logging.info('%s: entering positions %s, value=%f' % (timestamp, positions, value))
    return value, positions, position_sign


def position_close(timestamp, position_sign, positions, close_prices, cost_value):
    value = positions.dot(numpy.array(close_prices))
    logging.info('%s: solding out positions %s, value=%s' % (timestamp, positions, value))
    logging.info('-----> P&L: %f' % (value - cost_value))
    positions = numpy.zeros(len(close_prices))
    position_sign = 0.
    return positions, position_sign


def main(args):
    pyplot.style.use('ggplot')
    prices_path = os.sep.join(['..', 'resources','eod.pkl'])
    all_prices = pandas.read_pickle(prices_path)

    #securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
    securities = ['PCX/' + symbol for symbol in ['EMB','LQD','SPY']]
    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['date'] < date(2015, 1, 1)]
    delta = 2E-5

    initial_state_mean = numpy.zeros(len(securities))
    initial_state_covariance = numpy.ones((len(securities), len(securities)))
    observation_covariance = 1E-5
    trans_cov = delta / (1. - delta) * numpy.eye(len(securities))

    fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
    chart_bollinger = list()
    chart_beta = list()
    chart_positions = list()
    #disc_funcs = [HysteresisDiscretize(0.02, 0.4) for count in range(len(securities))]
    #disc_funcs = [lambda x: x for count in range(len(securities))]
    positions = numpy.zeros(len(securities))
    position_sign = 0.
    cost_value = 0.
    for row in in_sample_prices.iterrows():
        row_count, price_data = row
        timestamp = price_data['date']
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        result = fls.estimate(dependent_price, independent_prices.tolist() + [1.])
        dev = math.sqrt(result.var_output_error)

        portfolio_value = dependent_price - numpy.array(result.beta[:-1]).dot(independent_prices.values)

        weights = numpy.array([-1.] + result.beta[:-1])
        close_prices = [dependent_price] + independent_prices.tolist()
        if result.error > dev:
            if position_sign == -1.:
                positions, position_sign = position_close(timestamp, position_sign, positions, close_prices, cost_value)
                cost_value = 0.

            if position_sign != 1.:
                cost_value, positions, position_sign = position_enter(timestamp, 1., weights, close_prices)

        elif result.error < dev:
            if position_sign == 1.:
                positions, position_sign = position_close(timestamp, position_sign, positions, close_prices, cost_value)
                cost_value = 0.

            if position_sign != -1.:
                cost_value, positions, position_sign = position_enter(timestamp, -1., weights, close_prices)

        else:
            if position_sign == 1. and result.error < 0:
                positions, position_sign = position_close(timestamp, position_sign, positions, close_prices, cost_value)
                cost_value = 0.

            elif position_sign == -1. and result.error > 0:
                positions, position_sign = position_close(timestamp, position_sign, positions, close_prices, cost_value)
                cost_value = 0.

        samples = {'portfolio': portfolio_value, 'inf': -1.0 * dev, 'sup': 1.0 * dev, 'error': result.error}
        chart_bollinger.append(samples)

        beta_data = dict()
        for count, weight in enumerate(result.beta):
            beta_data['beta%d' % count] = weight

        chart_beta.append(beta_data)

    #pandas.DataFrame(chart_positions).plot(subplots=False)
    pandas.DataFrame(chart_beta).plot(subplots=True)
    pandas.DataFrame(chart_bollinger).plot(subplots=False)
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
