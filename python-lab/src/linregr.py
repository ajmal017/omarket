from pandas_datareader import data
from pykalman import KalmanFilter
import numpy
import pandas
import os
from matplotlib import pyplot
from datetime import date
from statsmodels.formula.api import OLS
from statsmodels.api import add_constant


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
    returns = output['signal'].resample('1D').pct_change()
    print(returns.std())

    # the number of 1 day intervals in 8 days
    #window_size = pandas.Timedelta('D') / pandas.Timedelta('1D')
    #df['std'] = pd.rolling_std(df['val'], window=window_size)

    ema = output['signal'].ewm(halflife=200).mean()
    output['ewma'] = ema
    output['bolsup'] = ema + 2. * returns.std() / numpy.math.sqrt(returns.size)
    output['bolinf'] = ema - 2. * returns.std() / numpy.math.sqrt(returns.size)
    output.plot()
    pyplot.show()


if __name__ == '__main__':
    main()