from matplotlib import pyplot
import pandas
from datetime import date
from fls import FlexibleLeastSquare
import math
import numpy
import os


def main():
    pyplot.style.use('ggplot')
    prices_path = os.sep.join(['..', 'resources','eod.pkl'])
    all_prices = pandas.read_pickle(prices_path)

    securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['date'] < date(2016, 1, 1)]
    delta = 2E-5

    initial_state_mean = numpy.zeros(len(securities))
    initial_state_covariance = numpy.ones((len(securities), len(securities)))
    observation_covariance = 1E-5
    trans_cov = delta / (1. - delta) * numpy.eye(len(securities))

    fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
    chart_bollinger = list()
    #chart_beta = list()
    chart_positions = list()
    for row in in_sample_prices.iterrows():
        timestamp, price_data = row
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        result = fls.estimate(dependent_price, independent_prices.tolist() + [1.])
        dev = math.sqrt(result.var_output_error)
        beta_data = dict()
        for count, beta in enumerate(result.beta):
            beta_data['beta%d' % count] = beta

        #chart_beta.append(beta_data)
        position = 0.
        portfolio_value = dependent_price - numpy.array(result.beta[:-1]).dot(independent_prices.values) - result.beta[-1]
        if result.error > dev:
            position = 1.

        elif result.error < dev:
            position = -1.

        samples = {'error': result.error, 'portfolio': portfolio_value, 'inf': -dev, 'sup': dev}
        chart_bollinger.append(samples)
        chart_positions.append({'position': position})

    pandas.DataFrame(chart_positions).plot(subplots=False)
    pandas.DataFrame(chart_bollinger).plot(subplots=False)
    pyplot.show()

if __name__ == "__main__":
    main()
