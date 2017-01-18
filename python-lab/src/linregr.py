from pandas_datareader import data
from pykalman import KalmanFilter
import numpy
import pandas
import os
from matplotlib import pyplot
from datetime import date
from statsmodels.formula.api import OLS
from statsmodels.api import add_constant

from fls import FlexibleLeastSquare


def main():
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
    print(regress.params)
    output = pandas.DataFrame()
    output['signal'] = regress.resid
    changes = output['signal'].resample('1D ').last().pct_change()

    v_epsilon = 0.001
    delta = 0.0001
    fls = FlexibleLeastSquare(len(securities) - 1, delta, v_epsilon)

    def fls_signal(row):
        target = row[0]
        factors = row[1:]
        result = fls.estimate(target, factors.values.tolist())
        beta_values = numpy.array([round(beta, 2) for beta in result.beta])
        estimate = factors.values.dot(beta_values[:-1]) + beta_values[-1]
        return target - estimate

    def fls_bollinger(row):
        target = row[0]
        factors = row[1:]
        result = fls.estimate(target, factors.values.tolist())
        return result.var_output_error

    output['signal0'] = in_sample_prices.apply(fls_signal, axis=1)
    output['sup'] = output['signal0'] + in_sample_prices.apply(fls_bollinger, axis=1)
    output['inf'] = output['signal0'] - in_sample_prices.apply(fls_bollinger, axis=1)

    # the number of 1 day intervals in 8 days
    #window_size = pandas.Timedelta('D') / pandas.Timedelta('1D')
    #df['std'] = pd.rolling_std(df['val'], window=window_size)

    ema = output['signal0'].ewm(halflife=200).mean()
    output['ewma'] = ema
    output['bolsup'] = ema + 1. * changes.std() / numpy.math.sqrt(changes.size)
    output['bolinf'] = ema - 1. * changes.std() / numpy.math.sqrt(changes.size)
    output[['signal0', 'sup', 'inf']].plot()

    def fls_beta(beta_id):
        def func(row):
            target = row[0]
            factors = row[1:]
            result = fls.estimate(target, factors.values.tolist())
            return result.beta[beta_id]

        return func

    beta_chart = pandas.DataFrame()
    beta_chart['beta0'] = in_sample_prices.apply(fls_beta(0), axis=1)
    beta_chart['beta1'] = in_sample_prices.apply(fls_beta(1), axis=1)
    beta_chart['beta2'] = in_sample_prices.apply(fls_beta(2), axis=1)
    beta_chart.plot()

    pyplot.show()


if __name__ == '__main__':
    main()