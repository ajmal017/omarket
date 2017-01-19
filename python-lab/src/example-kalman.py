from matplotlib import pyplot
import numpy as np
import pandas
from datetime import date
from pykalman import KalmanFilter
from fls import FlexibleLeastSquare
import math

def draw_date_coloured_scatterplot(etfs, prices):
    """
    Create a scatterplot of the two ETF prices, which is
    coloured by the date of the price to indicate the
    changing relationship between the sets of prices
    """
    # Create a yellow-to-red colourmap where yellow indicates
    # early dates and red indicates later dates
    plen = len(prices)
    colour_map = pyplot.cm.get_cmap('YlOrRd')
    colours = np.linspace(0.1, 1, plen)

    # Create the scatterplot object
    scatterplot = pyplot.scatter(
        prices[etfs[0]], prices[etfs[1]],
        s=30, c=colours, cmap=colour_map,
        edgecolor='k', alpha=0.8
    )

    # Add a colour bar for the date colouring and set the
    # corresponding axis tick labels to equal string-formatted dates
    colourbar = pyplot.colorbar(scatterplot)
    colourbar.ax.set_yticklabels(
        [str(p) for p in prices[::plen//9].index]
    )
    pyplot.xlabel(prices.columns[0])
    pyplot.ylabel(prices.columns[1])


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
        [prices[etfs[1]], np.ones(prices[etfs[1]].shape)]
    ).T[:, np.newaxis]

    kf = KalmanFilter(
        n_dim_obs=1,
        n_dim_state=2,
        initial_state_mean=initial_state_mean,
        initial_state_covariance=initial_state_covariance,
        transition_matrices=transition_matrices,
        observation_matrices=obs_mat,
        observation_covariance=observation_covariance,
        transition_covariance=trans_cov
    )

    state_means, state_covs = kf.filter(prices[etfs[0]].values)
    return state_means, state_covs


def draw_slope_intercept_changes(prices, state_means):
    """
    Plot the slope and intercept changes from the
    Kalman Filte calculated values.
    """
    pandas.DataFrame(
        dict(
            slope=state_means[:, 0],
            intercept=state_means[:, 1]
        ), index=prices.index
    ).plot(subplots=True)


def discretize(value, step, shift=0.):
    adj_value = value + shift
    return int(adj_value * (1. / step)) * step + 0.5 * step * (-1., 1.)[adj_value > 0]


if __name__ == "__main__":
    pyplot.style.use('ggplot')

    securities = ['PCX/' + symbol for symbol in ['BKLN','HYG']]

    # get adjusted close prices from Yahoo
    prices_path = '../../data/eod/eod.pkl'
    all_prices = pandas.read_pickle(prices_path)
    prices = all_prices[securities]
    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['date'] < date(2016, 1, 1)]

    delta = 1E-6

    draw_date_coloured_scatterplot(securities, in_sample_prices)
    initial_state_mean = np.zeros(2)
    initial_state_covariance = np.ones((2, 2))
    transition_matrices = np.eye(2)
    observation_covariance = 1E-4
    trans_cov = delta / (1. - delta) * np.eye(2)
    state_means, state_covs = calc_slope_intercept_kalman(securities, in_sample_prices,
                                                          initial_state_mean=initial_state_mean,
                                                          initial_state_covariance=initial_state_covariance,
                                                          transition_matrices=transition_matrices,
                                                          observation_covariance=observation_covariance,
                                                          trans_cov=trans_cov)
    draw_slope_intercept_changes(in_sample_prices, state_means)

    fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)
    charting = list()
    for row in in_sample_prices.iterrows():
        timestamp, price_data = row
        p1, p2 = price_data[securities[0]], price_data[securities[1]]
        result = fls.estimate(p1, [p2, 1.])
        dev = math.sqrt(result.var_output_error)
        samples = {'error': result.error, 'inf': -dev, 'sup': dev}
        discretitation_level = 1E-9
        beta0 = discretize(result.beta[0], discretitation_level)
        beta1 = discretize(result.beta[1], discretitation_level)
        #samples = {'slope': beta0, 'intercept': beta1}
        charting.append(samples)

    pandas.DataFrame(charting).plot(subplots=False)
    pyplot.show()