import unittest

import logging
import numpy
import pandas
import os
from pykalman import KalmanFilter
from fls import FlexibleLeastSquare


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
    obs_mat = numpy.vstack(
        [prices[etfs[1:]].values.T, numpy.ones(prices[etfs[1]].shape)]
    ).T[:, numpy.newaxis]

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


class KalmanTest(unittest.TestCase):

    @staticmethod
    def test_triple():
        securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
        in_sample_prices = pandas.read_pickle(os.sep.join(['..', 'resources', 'test_kalman.pickle']))
        delta = 2E-5

        dim = len(securities)
        initial_state_mean = numpy.zeros(dim)
        initial_state_covariance = numpy.ones((dim, dim))
        transition_matrices = numpy.eye(dim)
        observation_covariance = 1E-5
        trans_cov = delta / (1. - delta) * numpy.eye(dim)
        state_means, state_covs = calc_slope_intercept_kalman(securities, in_sample_prices,
                                                              initial_state_mean=initial_state_mean,
                                                              initial_state_covariance=initial_state_covariance,
                                                              transition_matrices=transition_matrices,
                                                              observation_covariance=observation_covariance,
                                                              trans_cov=trans_cov)
        fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
        chart_beta = list()
        for row in in_sample_prices.iterrows():
            timestamp, price_data = row
            dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
            result = fls.estimate(dependent_price, independent_prices.tolist() + [1.])
            beta_data = dict()
            for count, beta in enumerate(result.beta):
                beta_data['beta%d' % count] = beta

            chart_beta.append(beta_data)

        numpy.testing.assert_array_almost_equal(pandas.DataFrame(chart_beta[1:]).values, state_means[:-1], decimal=6)

    def test_triple_no_const(self):
        securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
        in_sample_prices = pandas.read_pickle(os.sep.join(['..', 'resources', 'test_kalman.pickle']))
        delta = 2E-5
        dim = len(securities) - 1
        initial_state_mean = numpy.zeros(dim)
        initial_state_covariance = numpy.ones((dim, dim))
        observation_covariance = 1E-5
        trans_cov = delta / (1. - delta) * numpy.eye(dim)
        fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
        chart_beta = list()
        for row in in_sample_prices.iterrows():
            timestamp, price_data = row
            dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
            result = fls.estimate(dependent_price, independent_prices.tolist())
            beta_data = dict()
            for count, beta in enumerate(result.beta):
                beta_data['beta%d' % count] = beta

            chart_beta.append(beta_data)

        self.assertTrue(numpy.isnan(chart_beta[0]['beta0']))
        self.assertTrue(numpy.isnan(chart_beta[0]['beta1']))
        self.assertDictEqual(chart_beta[-1], {'beta0': 0.2854202469956791, 'beta1': 0.3791521942363885})

    def test_triple_no_const_large_delta(self):
        securities = ['PCX/' + symbol for symbol in ['PFF','XLV','XRT']]
        in_sample_prices = pandas.read_pickle(os.sep.join(['..', 'resources', 'test_kalman.pickle']))
        delta = 0.99999
        dim = len(securities) - 1
        initial_state_mean = numpy.zeros(dim)
        initial_state_covariance = numpy.ones((dim, dim))
        observation_covariance = 1E-3
        trans_cov = delta / (1. - delta) * numpy.eye(dim)
        fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
        chart_beta = list()
        for row in in_sample_prices.iterrows():
            timestamp, price_data = row
            dependent_price, independent_prices = price_data[securities[0]], price_data[securities[1:]]
            result = fls.estimate(dependent_price, independent_prices.tolist())
            beta_data = dict()
            for count, beta in enumerate(result.beta):
                beta_data['beta%d' % count] = beta

            chart_beta.append(beta_data)

        self.assertTrue(numpy.isnan(chart_beta[0]['beta0']))
        self.assertTrue(numpy.isnan(chart_beta[0]['beta1']))
        self.assertDictEqual(chart_beta[-1], {'beta0': 0.2854202469956791, 'beta1': 0.3791521942363885})

if __name__ == '__main__':
    unittest.main()
