import pandas

from btplatform import BacktestHistory
from regression import RegressionModelOLS


class PortfolioDataCollector(object):

    def __init__(self):
        self._starting_equity = 0.
        self.fills_df = pandas.DataFrame()
        self.target_df = pandas.DataFrame()

    def add_strategy_data(self, securities, target_quantities, fills):
        self.fills_df = pandas.concat([self.fills_df, fills])
        if target_quantities is not None:
            strategy_new_target = {'securities': securities, 'target': target_quantities}
            self.target_df = pandas.concat([self.target_df, pandas.DataFrame(strategy_new_target)])

    def add_equity(self, equity):
        self._starting_equity += equity

    def get_new_targets(self):
        if self.target_df.size == 0:
            return self.target_df

        return self.target_df.set_index('securities')['target']

    def get_backtest_history(self):
        return BacktestHistory(self.fills_df, self._starting_equity)


class StrategyDataCollector(object):

    def __init__(self, securities, position_adjuster):
        self._target_quantities = None
        self._securities = securities
        self.chart_bollinger = list()
        self.chart_beta = list()
        self.chart_regression = list()
        self.position_adjuster = position_adjuster

    def get_closed_trades(self):
        return self.position_adjuster.get_closed_trades()

    def get_open_trades(self):
        return self.position_adjuster.get_open_trades()

    def set_target_quantities(self, target_quantities):
        self._target_quantities = target_quantities

    def get_target_quantities(self):
        return self._target_quantities

    def get_name(self):
        return ','.join([security.split('/')[1] for security in self._securities])

    def get_backtest_history(self):
        return BacktestHistory(self.position_adjuster.get_fills(), self.position_adjuster.start_equity)

    def get_summary(self):
        closed_trades = self.get_closed_trades()
        mean_trade = closed_trades['pnl'].mean()
        worst_trade = closed_trades['pnl'].min()
        count_trades = closed_trades['pnl'].count()
        max_drawdown = self.get_backtest_history().get_drawdown().max()['equity']
        final_equity = self.get_backtest_history().get_equity()['equity'][-1]
        summary = {
            'strategy': self.get_name(),
            'sharpe_ratio': self.get_backtest_history().get_sharpe_ratio(),
            'average_trade': mean_trade,
            'worst_trade': worst_trade,
            'count_trades': count_trades,
            'max_drawdown_pct': max_drawdown,
            'final_equity': final_equity
        }
        return summary

    def collect_after_close(self, strategy_runner):
        if strategy_runner.count_day > strategy_runner.warmup_period:
            signal_data = {
                'strategy': self.position_adjuster.get_name(),
                'date': strategy_runner.day,
                'level_inf': strategy_runner.position_adjuster.level_inf(),
                'level_sup': strategy_runner.position_adjuster.level_sup(),
                'signal': strategy_runner.strategy.get_state('signal')
            }
            self.chart_bollinger.append(signal_data)
            factors_data = strategy_runner.strategy.get_state('factors')
            beta_data = {'strategy': self.position_adjuster.get_name()}
            for count_factor, weight in enumerate(factors_data):
                beta_data['beta%d' % count_factor] = weight

            beta_data['date'] = strategy_runner.day
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
