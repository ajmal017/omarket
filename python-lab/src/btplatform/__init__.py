import logging
import math
import pandas
import numpy

from pnl import AverageCostProfitAndLoss


class ExecutionEngine(object):
    def __init__(self, securities, strategy_name):
        self._securities = securities
        self._securities_index = {security: count for count, security in enumerate(securities)}
        self._trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self._fills = list()
        self._strategy_name = strategy_name

    def execute(self, timestamp, count, fill_qty, price):
        trades_tracker = self._trades_tracker[self._securities[count]]
        if fill_qty != 0:
            trades_tracker.add_fill(fill_qty, price)

        fill_data = {'security': self._securities[count],
                     'strategy': self._strategy_name,
                     'date': timestamp,
                     'fill_qty': fill_qty,
                     'price': price,
                     'total_qty': trades_tracker.quantity,
                     'acquisition_cost': trades_tracker.acquisition_cost,
                     'average_price': trades_tracker.average_price,
                     'realized_pnl': trades_tracker.realized_pnl,
                     'unrealized_pnl': trades_tracker.calc_unrealized_pnl(price),
                     'market_value': trades_tracker.calc_market_value(price),
                     }
        self._fills.append(fill_data)

    def get_fills(self):
        """

        :return: historical trades data
        """
        return pandas.DataFrame(self._fills)

    def get_nav(self, prices):
        total_pnl = 0.
        for security in self._trades_tracker:
            pnl_calc = self._trades_tracker[security]
            price = prices[self._securities_index[security]]
            total_pnl += pnl_calc.calc_total_pnl(price)

        return total_pnl


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
        self._execution_engine = ExecutionEngine(securities, self.get_name())

    @property
    def start_equity(self):
        return self._start_equity

    def execute_trades(self, timestamp, quantities, prices):
        for count, quantity_price in enumerate(zip(quantities, prices)):
            target_quantity, price = quantity_price
            fill_qty = target_quantity - self._current_quantities[count]
            self._execution_engine.execute(timestamp, count, fill_qty, price)
            self._current_quantities[count] = target_quantity

    def evaluate_positions(self, timestamp, prices):
        for count, price in enumerate(prices):
            self._execution_engine.execute(timestamp, count, 0, price)

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
            logging.warning(
                'risk scale %d exceeding max risk scale: capping to %d' % (target_risk_scale, self._max_risk_scale))
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

    def get_name(self):
        return ','.join([code.split('/')[1] for code in self._securities])

    def get_fills(self):
        return self._execution_engine.get_fills()

    def get_closed_trades(self):
        return pandas.DataFrame(self._closed_trades)

    def get_open_trades(self):
        return pandas.DataFrame(self._open_trades)


def process_strategy(securities, strategy_runner, data_collector, prices_by_security):
    """

    :param securities:
    :param strategy_runner:
    :param data_collector:
    :param prices_by_security:
    :return:
    """

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

    for count_day, day in enumerate(sorted(dates)):
        px_open = prices_open[prices_open.index == day].values.transpose()[0]
        px_close = prices_close[prices_close.index == day].values.transpose()[0]
        px_close_adj = prices_close_adj[prices_close_adj.index == day].values.transpose()[0]
        dividends = prices_dividend[prices_dividend.index == day].values.transpose()[0]
        strategy_runner.on_open(day, px_open)
        strategy_runner.on_close(px_close)
        strategy_runner.on_after_close(dividends, px_close_adj, px_close)
        if strategy_runner.count_day > strategy_runner.warmup_period:
            position_adjuster = strategy_runner.position_adjuster
            strategy_name = position_adjuster.get_name()
            level_inf = strategy_runner.position_adjuster.level_inf()
            level_sup = strategy_runner.position_adjuster.level_sup()
            signal = strategy_runner.strategy.get_state('signal')
            factors_data = strategy_runner.strategy.get_state('factors')
            data_collector.collect_after_close(strategy_name, strategy_runner.day, level_inf, level_sup, signal, factors_data)

    data_collector.add_target_quantities(data_collector.position_adjuster.get_name(), strategy_runner.target_quantities)


class BacktestHistory(object):

    def __init__(self, backtest_history):
        self._backtest_history = backtest_history
        self._start_equity = 0.

    def set_start_equity(self, amount):
        self._start_equity = amount

    def get_equity(self):
        total_pnl = self.backtest_history[['date', 'realized_pnl', 'unrealized_pnl']].groupby(by=['date']).sum()
        total_pnl['equity'] = total_pnl['realized_pnl'] + total_pnl['unrealized_pnl'] + self._start_equity
        return total_pnl['equity']

    def get_holdings(self):
        holdings = self.backtest_history[['date', 'strategy', 'security', 'total_qty', 'market_value']]
        return holdings.reset_index(drop=True)

    def get_trades(self):
        trades_groups = self.backtest_history[['date', 'security', 'fill_qty']].groupby(['security', 'date'])
        return trades_groups.sum().unstack()['fill_qty'].fillna(0.).transpose()

    def get_return(self):
        return self.get_equity().pct_change()

    def get_gross_net_position(self):
        gross_net_positions = self.backtest_history[['date', 'market_value']].groupby(by=['date']).sum()
        gross_net_positions.columns = ['net_position']

        def abs_sum(group):
            return numpy.abs(group['market_value']).sum()

        gross_positions = self.backtest_history[['date', 'market_value']].groupby(by=['date']).apply(abs_sum)
        gross_net_positions['gross_position'] = gross_positions
        return gross_net_positions

    def get_sharpe_ratio(self):
        mean_return = self.get_equity().pct_change().mean()
        std_return = self.get_equity().pct_change().std()
        value = mean_return / std_return * math.sqrt(250)
        return value['equity']

    def get_drawdown(self):
        cum_returns = (1. + self.get_return()).cumprod()
        return 1. - cum_returns.div(cum_returns.cummax())

    @property
    def backtest_history(self):
        return self._backtest_history

