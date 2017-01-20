from matplotlib import pyplot
import numpy as np
import pandas
from datetime import date
from fls import FlexibleLeastSquare
import math
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

    initial_state_mean = np.zeros(len(securities))
    initial_state_covariance = np.ones((len(securities), len(securities)))
    observation_covariance = 1E-5
    trans_cov = delta / (1. - delta) * np.eye(len(securities))

    fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
    chart_bollinger = list()
    chart_beta = list()
    for row in in_sample_prices.iterrows():
        timestamp, price_data = row
        dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
        result = fls.estimate(dependent_price, independent_prices.tolist() + [1.])
        dev = math.sqrt(result.var_output_error)
        beta_data = dict()
        for count, beta in enumerate(result.beta):
            beta_data['beta%d' % count] = beta

        chart_beta.append(beta_data)
        samples = {'error': result.error, 'inf': -dev, 'sup': dev}
        chart_bollinger.append(samples)

    pandas.DataFrame(chart_beta).plot(subplots=True)
    pandas.DataFrame(chart_bollinger).plot(subplots=False)
    pyplot.show()

if __name__ == "__main__":
    main()
