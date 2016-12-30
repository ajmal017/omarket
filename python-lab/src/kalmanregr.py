from pandas_datareader import data
from pykalman import KalmanFilter
import numpy
import pandas
import os
from matplotlib import pyplot
from statsmodels.formula.api import OLS
from statsmodels.api import add_constant
from datetime import date


def main():
    pyplot.style.use('ggplot')

    secs = ['EWA', 'EWC']
    # get adjusted close prices from Yahoo
    prices_path = 'prices.pkl'
    if os.path.exists(prices_path):
        print('loading from cache')
        prices = pandas.read_pickle(prices_path)

    else:
        print('loading from web')
        prices = data.DataReader(secs, 'yahoo', '2011-12-28', '2016-12-28')['Adj Close']
        prices.to_pickle('prices.pkl')

    prices.to_csv('prices.csv')

    prices.reset_index(inplace=True)
    in_sample_prices = prices[prices['Date'] < date(2016, 1, 1)]
    out_sample_prices = prices[prices['Date'] >= date(2016, 1, 1)]
    prices = prices.set_index('Date')
    in_sample_prices = in_sample_prices.set_index('Date')
    out_sample_prices = out_sample_prices.set_index('Date')

    Y = in_sample_prices['EWC']
    X = add_constant(in_sample_prices['EWA'])

    regress = OLS(Y, X).fit()
    print(regress.params)

    # visualize the correlation between assest prices over time
    cm = pyplot.cm.get_cmap('jet')
    count = prices['EWA'].count()
    colors = numpy.linspace(0.1, 1, count)
    sc = pyplot.scatter(prices[prices.columns[0]], prices[prices.columns[1]], s=30, c=colors, cmap=cm, edgecolor='k', alpha=0.7)
    cb = pyplot.colorbar(sc)
    cb.ax.set_yticklabels([p.date() for p in prices[::count//9].index])
    pyplot.xlabel(prices.columns[0])
    pyplot.ylabel(prices.columns[1])

    delta = 1e-4
    process_noise = delta / (1 - delta) * numpy.eye(2)
    measurement_noise = 1e-5
    obs_mat = numpy.vstack([prices['EWA'], numpy.ones(prices['EWA'].shape)]).T[:, numpy.newaxis]
    initial_state_estimate = numpy.zeros(2)
    initial_error_covariance = numpy.ones((2, 2))
    kf = KalmanFilter(n_dim_obs=1, n_dim_state=2,
                      initial_state_mean=initial_state_estimate,
                      initial_state_covariance=initial_error_covariance,
                      transition_matrices=numpy.eye(2),
                      observation_matrices=obs_mat,
                      observation_covariance=measurement_noise,
                      transition_covariance=process_noise)

    state_means, state_covs = kf.filter(prices['EWC'].values)
    results = {'slope': state_means[:, 0], 'intercept': state_means[:, 1]}
    output_df = pandas.DataFrame(results, index=prices.index)
    output_df.plot(subplots=True)
    pyplot.show()

    # visualize the correlation between assest prices over time
    cm = pyplot.cm.get_cmap('jet')
    colors = numpy.linspace(0.1, 1, count)
    sc = pyplot.scatter(prices[prices.columns[0]], prices[prices.columns[1]], s=50, c=colors, cmap=cm, edgecolor='k', alpha=0.7)
    cb = pyplot.colorbar(sc)
    cb.ax.set_yticklabels([p.date() for p in prices[::count//9].index])
    pyplot.xlabel(prices.columns[0])
    pyplot.ylabel(prices.columns[1])

    # add regression lines
    step = 100
    xi = numpy.linspace(prices[prices.columns[0]].min(), prices[prices.columns[0]].max(), 2)
    count_states = state_means[::step].size
    colors_l = numpy.linspace(0.1, 1, count_states)
    i = 0
    for beta in state_means[::step]:
        pyplot.plot(xi, beta[0] * xi + beta[1], alpha=.2, lw=1, c=cm(colors_l[i]))
        i += 1

    pyplot.show()

    slopes = state_means.transpose()[0][:out_sample_prices.index.size].transpose()

    portfolio = out_sample_prices['EWC'] - out_sample_prices['EWA'] * slopes
    portfolio.plot()
    pyplot.show()

if __name__ == '__main__':
    main()
