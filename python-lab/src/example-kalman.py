from matplotlib import pyplot
import numpy as np
import pandas
from datetime import date
from pykalman import KalmanFilter
from fls import FlexibleLeastSquare
import math


def calc_slope_intercept_kalman(etfs, prices,
                                initial_state_mean,
                                initial_state_covariance,
                                transition_matrices,
                                observation_covariance,
                                trans_cov):
    """
    Utilise the Kalman Filter from the pyKalman package
    to calculate the slope and intercept of the regressed
    ETF prices.
    """
    obs_mat = np.vstack(
        [prices[etfs[1:]].values.T, np.ones(prices[etfs[1]].shape)]
    ).T[:, np.newaxis]

    kf = KalmanFilter(
        n_dim_obs=1,
        n_dim_state=len(etfs),
        initial_state_mean=initial_state_mean,
        initial_state_covariance=initial_state_covariance,
        transition_matrices=transition_matrices,
        observation_matrices=obs_mat,
        observation_covariance=observation_covariance,
        transition_covariance=trans_cov
    )

    state_means, state_covs = kf.filter(prices[etfs[0]].values)
    return state_means, state_covs


def main():

    pyplot.style.use('ggplot')

    securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]

    # get adjusted close prices from Yahoo
    prices_path = '../../data/eod/eod.pkl'
    all_prices = pandas.read_pickle(prices_path)
    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['date'] < date(2016, 1, 1)]
    in_sample_prices.to_pickle('../resources/test_kalman.pickle')
    delta = 2E-5

    initial_state_mean = np.zeros(len(securities))
    initial_state_covariance = np.ones((len(securities), len(securities)))
    transition_matrices = np.eye(len(securities))
    observation_covariance = 1E-5
    trans_cov = delta / (1. - delta) * np.eye(len(securities))
    state_means, state_covs = calc_slope_intercept_kalman(securities, in_sample_prices,
                                                          initial_state_mean=initial_state_mean,
                                                          initial_state_covariance=initial_state_covariance,
                                                          transition_matrices=transition_matrices,
                                                          observation_covariance=observation_covariance,
                                                          trans_cov=trans_cov)

    pandas.DataFrame(state_means).plot(subplots=True)
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
