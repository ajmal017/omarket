import logging
import math

import numpy
import pandas

from pnl import AverageCostProfitAndLoss
from regression import RegressionModelOLS


class ExecutionEngine(object):

    def __init__(self, securities):
        self._securities = securities
        self._securities_index = {security: count for count, security in enumerate(securities)}
        self._trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self._fills = list()

    def execute(self, timestamp, count, fill_qty, price):
        trades_tracker = self._trades_tracker[self._securities[count]]
        trades_tracker.add_fill(fill_qty, price)
        self._fills.append({'security': self._securities[count], 'date': timestamp, 'qty': fill_qty, 'price': price})

    def get_fills(self):
        return pandas.DataFrame(self._fills)

    def get_nav(self, prices):
        total_pnl = 0.
        for security in self._trades_tracker:
            pnl_calc = self._trades_tracker[security]
            price = prices[self._securities_index[security]]
            total_pnl += pnl_calc.get_total_pnl(price)

        return total_pnl


class PortfolioDataCollector(object):

    def __init__(self, position_adjuster):
        self.holdings_history = pandas.DataFrame()
        self.equity_history = pandas.DataFrame()
        self.chart_bollinger = list()
        self.chart_beta = list()
        self.chart_regression = list()
        self.position_adjuster = position_adjuster

    def historize_state(self, timestamp, signal_prices, market_prices):
        holdings = self.position_adjuster.get_holdings(signal_prices)
        holdings['date'] = timestamp
        holdings['strategy'] = self.position_adjuster.get_name()
        equity = self.position_adjuster.get_nav(market_prices)
        equity_df = pandas.DataFrame({'date': [timestamp], 'equity': [equity]})
        self.equity_history = pandas.concat([self.equity_history, equity_df])
        self.holdings_history = pandas.concat([self.holdings_history, holdings])

    def get_equity(self):
        return self.equity_history.set_index('date')

    def get_return(self):
        return self.get_equity().pct_change()

    def get_holdings_history(self):
        return self.holdings_history

    def get_net_position(self):
        net_positions = pandas.DataFrame(self.holdings_history.groupby(by=['date'])['position'].sum())
        net_positions.columns = ['net_position']
        return net_positions

    def get_gross_position(self):

        def abs_sum(group):
            return numpy.abs(group).sum()

        gross_positions = pandas.DataFrame(self.holdings_history.groupby(by=['date'])['position'].apply(abs_sum))
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

    def get_fills(self):
        return self.position_adjuster.get_fills()

    def add_bollinger(self, signal_data):
        self.chart_bollinger.append(signal_data)

    def add_factors(self, factors_data, day):
        beta_data = dict()
        for count_factor, weight in enumerate(factors_data):
            beta_data['beta%d' % count_factor] = weight

        beta_data['date'] = day
        self.chart_beta.append(beta_data)


class PositionAdjuster(object):

    def __init__(self, securities, max_net_position, max_gross_position, max_risk_scale, start_equity, step_size):
        self._securities = securities
        self._current_quantities = [0.] * len(securities)
        self._max_net_position = max_net_position
        self._max_gross_position = max_gross_position
        self._max_risk_scale = max_risk_scale
        self._step_size = step_size
        self._start_equity = start_equity
        self._current_risk_scale = 0.
        self._signal_zone = 0
        self._deviation = 0.
        self._open_trades = list()
        self._closed_trades = list()
        self._execution_engine = ExecutionEngine(securities)

    def execute_trades(self, timestamp, quantities, prices):
        for count, quantity_price in enumerate(zip(quantities, prices)):
            target_quantity, price = quantity_price
            fill_qty = target_quantity - self._current_quantities[count]
            self._execution_engine.execute(timestamp, count, fill_qty, price)
            self._current_quantities[count] = target_quantity

    def update_target_positions(self, timestamp, signal, deviation, weights, market_prices):
        self._deviation = deviation * self._step_size
        movement = 0
        if signal >= self.level_sup():
            movement = 1

        if signal <= self.level_inf():
            movement = -1

        self._signal_zone += movement
        quantities = None
        if movement != 0:
            scaling = self._update_risk_scaling(timestamp, weights, market_prices)
            quantities = list()
            for count, weight_price in enumerate(zip(weights, market_prices)):
                weight, price = weight_price
                target_position = scaling * self._current_risk_scale * weight
                target_quantity = round(target_position / price)
                quantities.append(target_quantity)

        return quantities

    def _update_risk_scaling(self, timestamp, weights, market_prices):
        target_risk_scale = self._signal_zone
        if abs(target_risk_scale) > self._max_risk_scale:
            logging.warning('risk scale %d exceeding max risk scale: capping to %d' % (target_risk_scale, self._max_risk_scale))
            if target_risk_scale > 0:
                target_risk_scale = self._max_risk_scale

            elif target_risk_scale < 0:
                target_risk_scale = -self._max_risk_scale

        if self._current_risk_scale == target_risk_scale:
            logging.warning('position already at specified risk scale: ignoring')
            return

        logging.debug('moving to position: %d at %s' % (target_risk_scale, timestamp.strftime('%Y-%m-%d')))
        scaling = 0
        if target_risk_scale != 0:
            equity = self.get_nav(market_prices)
            scaling_gross = equity * self._max_gross_position / numpy.max(numpy.abs(weights))
            scaling_net = equity * self._max_net_position / numpy.abs(numpy.sum(weights))
            scaling = min(scaling_net, scaling_gross)

        self._handle_trades(timestamp, target_risk_scale, market_prices)
        self._current_risk_scale = target_risk_scale
        return scaling

    def _handle_trades(self, timestamp, target_risk_scale, prices):
        """
        Keeps track of high-level trades.

        :param timestamp:
        :param target_risk_scale:
        :param prices:
        :return:
        """
        steps_count = int(abs(target_risk_scale) - abs(self._current_risk_scale))
        if steps_count > 0:
            logging.debug('opening %d trade(s)' % steps_count)
            strategy_equity = self.get_cumulated_pnl(prices)
            for trade_count in range(steps_count):
                self._open_trades.append({'open': timestamp,
                                         'risk_level': target_risk_scale,
                                         'equity_start': strategy_equity,
                                         'equity_end': strategy_equity,
                                         'pnl': 0.
                                          })

        else:
            logging.debug('closing %d trade(s)' % abs(steps_count))
            for trade_count in range(abs(steps_count)):
                trade_result = self._open_trades.pop()
                trade_result['close'] = timestamp
                self._closed_trades.append(trade_result)

            # when multiple risk levels attributes whole pnl to last one
            self._closed_trades[-1]['equity_end'] = self.get_cumulated_pnl(prices)
            self._closed_trades[-1]['pnl'] = self._closed_trades[-1]['equity_end'] - self._closed_trades[-1][
                'equity_start']

    def level_inf(self):
        return (self._signal_zone - 1) * self._deviation

    def level_sup(self):
        return (self._signal_zone + 1) * self._deviation

    def get_cumulated_pnl(self, prices):
        return self._execution_engine.get_nav(prices)

    def get_nav(self, prices):
        return self._start_equity + self.get_cumulated_pnl(prices)

    def get_holdings(self, prices):
        positions = numpy.array(self._current_quantities) * prices
        return pandas.DataFrame({'security': self._securities, 'quantity': self._current_quantities, 'position': positions})

    def get_name(self):
        return ','.join(self._securities)

    def get_fills(self):
        return self._execution_engine.get_fills()

    def get_closed_trades(self):
        return pandas.DataFrame(self._closed_trades)

    def get_open_trades(self):
        return pandas.DataFrame(self._open_trades)


class MeanReversionStrategy(object):

    def __init__(self, securities, lookback_period):
        #regression0 = RegressionModelFLS(securities, delta=5E-6, with_constant_term=False)
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
            self.target_quantities = self.position_adjuster.update_target_positions(self.day, signal, deviation, weights, prices_close)

        self.last_phase = 'AfterClose'

    def collect_strategy_data(self, data_collector, prices_close_adj, prices_close):
        # statistics
        data_collector.historize_state(self.day, prices_close_adj, prices_close)
        if self.count_day > self.warmup_period:
            signal_data = {
                'date': self.day,
                'level_inf': self.position_adjuster.level_inf(),
                'level_sup': self.position_adjuster.level_sup(),
                'signal': self.strategy.get_state('signal')
            }
            data_collector.add_bollinger(signal_data)
            data_collector.add_factors(self.strategy.get_state('factors'), self.day)


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

    position_adjuster = PositionAdjuster(securities, max_net_position, max_gross_position, max_risk_scale, start_equity, step_size)
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

    data_collector = PortfolioDataCollector(position_adjuster)
    strategy_runner = MeanReversionStrategyRunner(securities, strategy, warmup_period, position_adjuster)
    for count_day, day in enumerate(sorted(dates)):
        strategy_runner.on_open(day, prices_open[prices_open.index == day].values.transpose()[0])
        strategy_runner.on_close(prices_close[prices_close.index == day].values.transpose()[0])
        strategy_runner.on_after_close(prices_dividend[prices_dividend.index == day].values.transpose()[0],
                                       prices_close_adj[prices_close_adj.index == day].values.transpose()[0],
                                       prices_close[prices_close.index == day].values.transpose()[0]
                                       )
        strategy_runner.collect_strategy_data(
            data_collector,
            prices_close_adj[prices_close_adj.index == day].values.transpose()[0],
            prices_close[prices_close.index == day].values.transpose()[0])

    closed_trades = data_collector.get_closed_trades()
    mean_trade = closed_trades['pnl'].mean()
    worst_trade = closed_trades['pnl'].min()
    count_trades = closed_trades['pnl'].count()
    max_drawdown = data_collector.get_drawdown().max()['equity']
    summary = {
        'portfolio': ','.join([security.split('/')[1] for security in securities]),
        'sharpe_ratio': data_collector.get_sharpe_ratio(),
        'average_trade': mean_trade,
        'worst_trade': worst_trade,
        'count_trades': count_trades,
        'max_drawdown_pct': max_drawdown
    }
    result = {
        'summary': summary,
        'bollinger': pandas.DataFrame(data_collector.chart_bollinger).set_index('date'),
        'factors': pandas.DataFrame(data_collector.chart_beta).set_index('date'),
        'equity': data_collector.get_equity(),
        'net_position': data_collector.get_net_position(),
        'gross_position': data_collector.get_gross_position(),
        'holdings': data_collector.get_holdings_history(),
        'fills': data_collector.get_fills(),
        'next_target_quantities': strategy_runner.target_quantities
    }
    logging.info('summary: %s' % str(summary))
    return result