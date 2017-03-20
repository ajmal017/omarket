import pandas

from btplatform import BacktestHistory
from regression import RegressionModelOLS


class PortfolioDataCollector(object):

    def __init__(self):
        self._starting_equity = 0.
        self._fills_df = pandas.DataFrame()
        self._target_df = pandas.DataFrame()

    def add_strategy_data(self, securities, target_quantities, fills):
        self._fills_df = pandas.concat([self._fills_df, fills])
        if target_quantities:
            strategy_new_target = {'securities': securities, 'target': target_quantities}
            self._target_df = pandas.concat([self._target_df, pandas.DataFrame(strategy_new_target)])

    def add_equity(self, equity):
        self._starting_equity += equity

    @property
    def new_targets(self):
        if self._target_df.size == 0:
            return self._target_df

        return self._target_df.set_index('securities')['target']

    @property
    def starting_equity(self):
        return self._starting_equity

    @property
    def fills_df(self):
        return self._fills_df


class StrategyDataCollector(object):

    def __init__(self, strategy_name):
        self._target_quantities = dict()
        self._strategy_name = strategy_name
        self.chart_bollinger = list()
        self.chart_beta = list()
        self.chart_regression = list()

    def add_target_quantities(self, strategy, target_quantities):
        self._target_quantities[strategy] = target_quantities

    def get_target_quantities(self, strategy):
        return self._target_quantities[strategy]

    def collect_after_close(self, strategy_name, day, level_inf, level_sup, signal, factors_data):
            signal_data = {
                'strategy': strategy_name,
                'date': day,
                'level_inf': level_inf,
                'level_sup': level_sup,
                'signal': signal
            }
            self.chart_bollinger.append(signal_data)
            beta_data = {'strategy': strategy_name}
            for count_factor, weight in enumerate(factors_data):
                beta_data['beta%d' % count_factor] = weight

            beta_data['date'] = day
            self.chart_beta.append(beta_data)

    def get_factors(self, strategy):
        beta_df = pandas.DataFrame(self.chart_beta)
        return beta_df[beta_df['strategy'] == strategy].set_index('date', drop=True)

    def get_bollinger(self, strategy):
        bollinger_df = pandas.DataFrame(self.chart_bollinger)
        return bollinger_df[bollinger_df['strategy'] == strategy].set_index('date', drop=True)


class MeanReversionStrategy(object):

    def __init__(self, securities, lookback_period):
        # regression0 = RegressionModelFLS(securities, delta=5E-6, with_constant_term=False)
        self._securities = securities
        self.regression = RegressionModelOLS(securities, with_constant_term=False, lookback_period=lookback_period)
        self.states = {
            'deviation': None,
            'weights': None,
            'signal': None,
            'factors': None,
        }

    def compute_signal(self, prices_close_adj, within_warmup_period):
        """

        :param prices_close_adj:
        :param within_warmup_period: flag signaling if still within warmup period
        :return:
        """
        dependent_price, independent_prices = prices_close_adj[0], prices_close_adj[1:]
        self.regression.add_samples(dependent_price, independent_prices.tolist())
        if not within_warmup_period:
            self.regression.compute_regression()
            self.states['deviation'] = self.regression.get_residual_error()
            self.states['weights'] = self.regression.get_weights()
            self.states['signal'] = self.regression.get_residual()
            self.states['factors'] = self.regression.get_factors()

    def get_state(self, name):
        return self.states[name]

    def get_strategy_name(self):
        return ','.join(self._securities)


class MeanReversionStrategyRunner(object):

    def __init__(self, securities, strategy, warmup_period, position_adjuster):
        self.daily_valuation = True
        self.securities = securities
        self.day = None
        self.last_phase = 'AfterClose'
        self.strategy = strategy
        self.warmup_period = warmup_period
        self.count_day = 0
        self.target_quantities = None
        self.position_adjuster = position_adjuster

    def on_open(self, day, prices_open):
        assert self.last_phase == 'AfterClose'
        self.day = day
        self.count_day += 1

        if self.count_day > self.warmup_period:
            # on-open market orders
            if self.target_quantities is not None:
                self.position_adjuster.execute_trades(day, self.target_quantities, prices_open)

            elif self.daily_valuation:
                self.position_adjuster.evaluate_positions(day, prices_open)

        self.last_phase = 'Open'

    def on_close(self, prices_close):
        assert self.last_phase == 'Open'
        self.last_phase = 'Close'

    def on_after_close(self, dividends, prices_close_adj, prices_close):
        assert self.last_phase == 'Close'
        within_warmup_period = self.count_day < self.warmup_period
        self.strategy.compute_signal(prices_close_adj, within_warmup_period)
        if not within_warmup_period:
            signal = self.strategy.get_state('signal')
            deviation = self.strategy.get_state('deviation')
            weights = self.strategy.get_state('weights')
            self.target_quantities = self.position_adjuster.update_target_positions(self.day, signal, deviation,
                                                                                    weights, prices_close)

        self.last_phase = 'AfterClose'

    def get_strategy_name(self):
        return self.strategy.get_strategy_name()