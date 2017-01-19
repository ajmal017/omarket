import argparse
import logging
from pykalman import KalmanFilter
import numpy
import pandas
from matplotlib import pyplot
from datetime import date
from statsmodels.formula.api import OLS
from statsmodels.api import add_constant
import math

from fls import FlexibleLeastSquare, DynamicLinearRegression


def calc_slope_intercept_kalman(etfs, prices, delta, v_epsilon):
    """
    Utilise the Kalman Filter from the pyKalman package
    to calculate the slope and intercept of the regressed
    ETF prices.
    """
    trans_cov = delta / (1 - delta) * numpy.eye(len(etfs))

    obs_mat = numpy.vstack([prices[etfs[1:]].values.T, numpy.ones(prices[etfs[1]].shape)]).T[:, numpy.newaxis]
    kf = KalmanFilter(
        n_dim_obs=1,
        n_dim_state=len(etfs),
        initial_state_mean=numpy.zeros(len(etfs)),
        initial_state_covariance=numpy.identity((len(etfs))),
        transition_matrices=numpy.eye(len(etfs)),
        observation_matrices=obs_mat,
        observation_covariance=v_epsilon,
        transition_covariance=trans_cov
    )
    inputs = prices[etfs[0]]
    state_means, state_covs = kf.filter(inputs.values)
    return state_means, state_covs


def discretize(value, step, shift=0.):
    adj_value = value + shift
    return int(adj_value * (1. / step)) * step + 0.5 * step * (-1., 1.)[adj_value > 0]


def main(args):
    pyplot.style.use('ggplot')

    securities = ['PCX/' + symbol for symbol in ['BKLN','HYG','JNK']]

    # get adjusted close prices from Yahoo
    prices_path = '../../data/eod/eod.pkl'
    all_prices = pandas.read_pickle(prices_path)
    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['date'] < date(2016, 1, 1)]
    out_sample_prices = prices[prices['date'] >= date(2016, 1, 1)]

    in_sample_prices = in_sample_prices.set_index('date')
    out_sample_prices = out_sample_prices.set_index('date')

    Y = in_sample_prices[securities[0]]
    X = add_constant(in_sample_prices[securities[1:]])

    regress = OLS(Y, X).fit()
    logging.info('lin regression results:\n%s' % str(regress.params))
    output = pandas.DataFrame()
    #output['signal'] = regress.resid
    output['target'] = Y
    output['estim'] = regress.params['const'] + in_sample_prices[securities[1]] * regress.params[securities[1]] + in_sample_prices[securities[2]] * regress.params[securities[2]]
    output.plot()
    v_epsilon = 1.
    delta = 1E-2

    fls = DynamicLinearRegression(len(securities), delta, v_epsilon)
    charting = list()
    for row in in_sample_prices.iterrows():
        timestamp, price_data = row
        p1, p2, p3 = price_data[securities[0]], price_data[securities[1]], price_data[securities[2]]
        result = fls.estimate(p1, [p2, p3, 1.])
        dev = math.sqrt(result.var_output_error)
        samples = {'error': result.error, 'inf': -dev, 'sup': dev}
        discretitation_level = 0.01
        beta0 = discretize(result.beta[0], discretitation_level)
        beta1 = discretize(result.beta[1], discretitation_level)
        beta2 = discretize(result.beta[2], discretitation_level)
        #samples = {'estim': p2 * beta0 + p3 * beta1 + beta2, 'target': p1}
        charting.append(samples)

    pandas.DataFrame(charting).plot()

    def fls_beta(beta_id):
        def func(row):
            target = row[securities[0]]
            factors = row[securities[1:]]
            result = fls.estimate(target, factors.tolist() + [1.])
            return discretize(result.beta[beta_id], step=0.05)

        return func

    beta_chart = pandas.DataFrame()
    beta_chart['beta0'] = in_sample_prices.apply(fls_beta(0), axis=1)
    beta_chart['beta1'] = in_sample_prices.apply(fls_beta(1), axis=1)
    beta_chart['beta2'] = in_sample_prices.apply(fls_beta(2), axis=1)
    beta_chart.plot()

    pyplot.show()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('draft.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='draft script.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    args = parser.parse_args()
    #main(args)
    values = numpy.linspace(-1, 1, 10000)
    df = pandas.DataFrame(values, columns=['in'])
    #df['disc'] = df['in'].apply(lambda x: discretize(x, 0.5))
    #df['disc0'] = df['in'].apply(lambda x: discretize(x, 0.5, shift=0.05))
    #df['disc1'] = df['in'].apply(lambda x: discretize(x, 0.5, shift=-0.05))
    df['disc1'] = df['in'].apply(lambda x: discretize(x, 0.5, 0.1))
    df['disc2'] = df['in'].apply(lambda x: discretize(x, 0.5, -0.1))
    df.plot()
    pyplot.show()

