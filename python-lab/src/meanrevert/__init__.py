import math

import numpy
import pandas

from btplatform import PositionAdjuster
from regression import RegressionModelOLS


class PortfolioDataCollector(object):

    def __init__(self):
        self._starting_equity = 0.
        self.trades_pnl = pandas.DataFrame()
        self.target_df = pandas.DataFrame()

    def add_strategy_data(self, data_collection):
        self.trades_pnl = pandas.concat([self.trades_pnl, data_collection.get_trades_pnl()])
        if data_collection.get_target_quantities() is not None:
            yahoo_codes = data_collection._securities
            strategy_new_target = {'securities': yahoo_codes, 'target': data_collection.get_target_quantities()}
            self.target_df = pandas.concat([self.target_df, pandas.DataFrame(strategy_new_target)])

    def add_equity(self, equity):
        self._starting_equity += equity

    def get_new_targets(self):
        if self.target_df.size == 0:
            return self.target_df

        return self.target_df.set_index('securities')['target']

    def get_holdings(self):
        holdings = self._get_trades_pnl()[['date', 'strategy', 'security', 'total_qty', 'market_value']]
        return holdings.reset_index(drop=True)

    def get_trades(self):
        trades_groups = self._get_trades_pnl()[['date', 'security', 'fill_qty']].groupby(['security', 'date'])
        return trades_groups.sum().unstack()['fill_qty'].transpose()

    def get_equity(self):
        pnl_details = self._get_trades_pnl()[['date', 'unrealized_pnl', 'realized_pnl']].groupby(by=['date']).sum()
        pnl_total = pnl_details['unrealized_pnl'] + pnl_details['realized_pnl'] + self._starting_equity
        pnl_total.name = 'equity'
        return pnl_total

    def _get_trades_pnl(self):
        columns = ['date', 'strategy', 'security', 'fill_qty', 'price', 'total_qty', 'average_price',
                   'acquisition_cost', 'market_value', 'realized_pnl', 'unrealized_pnl']
        return self.trades_pnl.reset_index(drop=True)[columns]


class StrategyDataCollector(object):

    def __init__(self, securities, position_adjuster):
        self._target_quantities = None
        self._securities = securities
        self.chart_bollinger = list()
        self.chart_beta = list()
        self.chart_regression = list()
        self.position_adjuster = position_adjuster

    def get_equity(self):
        total_pnl = self.get_trades_pnl()[['date', 'realized_pnl', 'unrealized_pnl']].groupby(by=['date']).sum()
        start_equity = self.position_adjuster.start_equity
        total_pnl['equity'] = total_pnl['realized_pnl'] + total_pnl['unrealized_pnl'] + start_equity
        return total_pnl['equity']

    def get_return(self):
        return self.get_equity().pct_change()

    def get_net_position(self):
        net_positions = self.get_trades_pnl()[['date', 'market_value']].groupby(by=['date']).sum()
        net_positions.columns = ['net_position']
        return net_positions

    def get_gross_position(self):

        def abs_sum(group):
            return numpy.abs(group['market_value']).sum()

        gross_positions = self.get_trades_pnl()[['date', 'market_value']].groupby(by=['date']).apply(abs_sum)
        gross_positions.columns = ['gross_position']
        return gross_positions

    def get_sharpe_ratio(self):
        mean_return = self.get_equity().pct_change().mean()
        std_return = self.get_equity().pct_change().std()
        value = mean_return / std_return * math.sqrt(250)
        return value['equity']

    def get_drawdown(self):
        cum_returns = (1. + self.get_return()).cumprod()
        return 1. - cum_returns.div(cum_returns.cummax())

    def get_closed_trades(self):
        return self.position_adjuster.get_closed_trades()

    def set_target_quantities(self, target_quantities):
        self._target_quantities = target_quantities

    def get_target_quantities(self):
        return self._target_quantities

    def get_name(self):
        return ','.join([security.split('/')[1] for security in self._securities])

    def get_trades_pnl(self):
        return self.position_adjuster.get_fills()

    def get_summary(self):
        closed_trades = self.get_closed_trades()
        mean_trade = closed_trades['pnl'].mean()
        worst_trade = closed_trades['pnl'].min()
        count_trades = closed_trades['pnl'].count()
        max_drawdown = self.get_drawdown().max()['equity']
        final_equity = self.get_equity()['equity'][-1]
        summary = {
            'strategy': self.get_name(),
            'sharpe_ratio': self.get_sharpe_ratio(),
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
                'date': strategy_runner.day,
                'level_inf': strategy_runner.position_adjuster.level_inf(),
                'level_sup': strategy_runner.position_adjuster.level_sup(),
                'signal': strategy_runner.strategy.get_state('signal')
            }
            self._add_bollinger(signal_data)
            self._add_factors(strategy_runner.strategy.get_state('factors'), strategy_runner.day)

    def _add_bollinger(self, signal_data):
        self.chart_bollinger.append(signal_data)

    def _add_factors(self, factors_data, day):
        beta_data = dict()
        for count_factor, weight in enumerate(factors_data):
            beta_data['beta%d' % count_factor] = weight

        beta_data['date'] = day
        self.chart_beta.append(beta_data)

    def get_factors(self):
        return pandas.DataFrame(self.chart_beta).set_index('date', drop=True)

    def get_bollinger(self):
        return pandas.DataFrame(self.chart_bollinger).set_index('date', drop=True)


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


def process_strategy(securities, strategy, warmup_period, prices_by_security,
                     step_size, start_equity, max_net_position, max_gross_position, max_risk_scale):
    """

    :param securities:
    :param strategy:
    :param warmup_period:
    :param prices_by_security:
    :param step_size:
    :param start_equity:
    :param max_net_position:
    :param max_gross_position:
    :param max_risk_scale:
    :return:
    """

    position_adjuster = PositionAdjuster(securities, max_net_position, max_gross_position, max_risk_scale, start_equity,
                                         step_size)
    dates = set()
    prices_open = pandas.DataFrame()
    prices_close = pandas.DataFrame()
    prices_close_adj = pandas.DataFrame()
    prices_dividend = pandas.DataFrame()
    for security in securities:
        security_prices = prices_by_security[security]
        prices_open = pandas.concat([prices_open, security_prices['open']])
        prices_close = pandas.concat([prices_close, security_prices['close']])
        prices_close_adj = pandas.concat([prices_close_adj, security_prices['close adj']])
        prices_dividend = pandas.concat([prices_dividend, security_prices['dividend']])
        dates = dates.union(set(security_prices.index.values.tolist()))

    data_collector = StrategyDataCollector(securities, position_adjuster)
    strategy_runner = MeanReversionStrategyRunner(securities, strategy, warmup_period, position_adjuster)
    for count_day, day in enumerate(sorted(dates)):
        px_open = prices_open[prices_open.index == day].values.transpose()[0]
        px_close = prices_close[prices_close.index == day].values.transpose()[0]
        px_close_adj = prices_close_adj[prices_close_adj.index == day].values.transpose()[0]
        dividends = prices_dividend[prices_dividend.index == day].values.transpose()[0]
        strategy_runner.on_open(day, px_open)
        strategy_runner.on_close(px_close)
        strategy_runner.on_after_close(dividends, px_close_adj, px_close)
        data_collector.collect_after_close(strategy_runner)

    data_collector.set_target_quantities(strategy_runner.target_quantities)
    return data_collector
