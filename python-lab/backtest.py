import argparse
import logging
import math

import numpy
import os
from datetime import date
from matplotlib import pyplot
import pandas
from pnl import AverageCostProfitAndLoss
from pricetools import load_prices
from regression import RegressionModelOLS

class ExecutionEngine(object):
    pass


class PositionAdjuster(object):

    def __init__(self, securities, max_net_position, max_gross_position, max_risk_scale, start_equity, step_size):
        self.securities = securities
        self.securities_index = {security: count for count, security in enumerate(securities)}
        self.trades_tracker = {security: AverageCostProfitAndLoss() for security in securities}
        self.current_quantities = [0.] * len(securities)
        self.max_net_position = max_net_position
        self.max_gross_position = max_gross_position
        self.max_risk_scale = max_risk_scale
        self._step_size = step_size
        self.start_equity = start_equity
        self.equity = 0.
        self.current_risk_scale = 0.
        self.current_level = 0.
        self.signal_zone = 0
        self.deviation = 0.
        self.open_trades = list()
        self.closed_trades = list()
        self._fills = list()
        self.equity_history = list()
        self.net_position_history = list()
        self.gross_position_history = list()
        self.positions_history = list()
        self.holdings_history = list()

    def execute_trades(self, timestamp, quantities, prices):
        for count, quantity_price in enumerate(zip(quantities, prices)):
            target_quantity, price = quantity_price
            trades_tracker = self.trades_tracker[self.securities[count]]
            fill_qty = target_quantity - self.current_quantities[count]
            trades_tracker.add_fill(fill_qty, price)
            self._fills.append({'security': self.securities[count], 'date': timestamp, 'qty': fill_qty, 'price': price})
            self.current_quantities[count] = target_quantity

    def get_fills(self):
        return pandas.DataFrame(self._fills)

    def update_risk_scaling(self, timestamp, weights, prices, risk_scale):
        if abs(risk_scale) > self.max_risk_scale:
            logging.warning('risk scale %d exceeding max risk scale: capping to %d' % (risk_scale, self.max_risk_scale))
            if risk_scale > 0:
                risk_scale = self.max_risk_scale

            elif risk_scale < 0:
                risk_scale = -self.max_risk_scale

        if self.current_risk_scale == risk_scale:
            logging.warning('position already at specified risk scale: ignoring')
            return

        logging.debug('moving to position: %d at %s' % (risk_scale, timestamp.strftime('%Y-%m-%d')))
        scaling = 0
        if risk_scale != 0:
            scaling_gross = self.equity * self.max_gross_position / numpy.max(numpy.abs(weights))
            scaling_net = self.equity * self.max_net_position / numpy.abs(numpy.sum(weights))
            scaling = min(scaling_net, scaling_gross)

        steps_count = int(abs(risk_scale) - abs(self.current_risk_scale))
        if steps_count > 0:
            logging.debug('opening %d trade(s)' % steps_count)
            strategy_equity = self.get_nav(prices)
            for trade_count in range(steps_count):
                self.open_trades.append({'open': timestamp,
                                         'risk_level': risk_scale,
                                         'equity_start': strategy_equity,
                                         'equity_end': strategy_equity,
                                         'pnl': 0.
                                         })

        else:
            logging.debug('closing %d trade(s)' % abs(steps_count))
            for trade_count in range(abs(steps_count)):
                trade_result = self.open_trades.pop()
                trade_result['close'] = timestamp
                self.closed_trades.append(trade_result)

            # when multiple risk levels attributes whole pnl to last one
            self.closed_trades[-1]['equity_end'] = self.get_nav(prices)
            self.closed_trades[-1]['pnl'] = self.closed_trades[-1]['equity_end'] - self.closed_trades[-1]['equity_start']

        self.current_risk_scale = risk_scale

        quantities = list()
        for count, weight_price in enumerate(zip(weights, prices)):
            weight, price = weight_price
            target_position = scaling * risk_scale * weight
            target_quantity = round(target_position / price)
            quantities.append(target_quantity)

        return quantities

    def update_state(self, timestamp, deviation, market_prices):
        self.deviation = deviation * self._step_size
        if deviation == 0.:
            return

        equity = self.get_nav(market_prices) + self.start_equity
        self.update_equity(equity)
        self.equity_history.append({'date': timestamp,
                                    'equity': equity})

        positions = self.get_positions(market_prices)
        net_positions = positions.sum()
        gross_positions = numpy.abs(positions).sum()
        self.net_position_history.append({'date': timestamp, 'net_position': net_positions})
        self.gross_position_history.append({'date': timestamp,
                                            'gross_position': gross_positions,
                                            'margin_call': equity / 0.25,
                                            'margin_warning': equity / 0.4})
        holdings = self.get_holdings()
        for security in holdings:
            holdings_data = {'date': timestamp, 'strategy': self.get_name()}
            holdings_data['quantity'] = holdings[security]
            holdings_data['security'] = security
            self.holdings_history.append(holdings_data)

        positions = self.get_position_securities(market_prices)
        for security in positions:
            positions_data = {'date': timestamp, 'strategy': self.get_name()}
            positions_data['position'] = positions[security]
            positions_data['security'] = security
            self.positions_history.append(positions_data)

    def update_target_positions(self, timestamp, signal, weights, market_prices):
        movement = 0
        if signal >= self.level_sup():
            movement = 1

        if signal <= self.level_inf():
            movement = -1

        self.signal_zone += movement
        target_quantities = None
        if movement != 0:
            target_quantities = self.update_risk_scaling(timestamp, weights, market_prices, self.signal_zone)

        return target_quantities

    def level_inf(self):
        return (self.signal_zone - 1) * self.deviation

    def level_sup(self):
        return (self.signal_zone + 1) * self.deviation

    def get_nav(self, prices):
        total_pnl = 0.
        for security in self.trades_tracker:
            pnl_calc = self.trades_tracker[security]
            price = prices[self.securities_index[security]]
            total_pnl += pnl_calc.get_total_pnl(price)

        return total_pnl

    def get_positions(self, prices):
        return numpy.array(self.current_quantities) * prices

    def get_position_securities(self, prices):
        return dict(zip(self.securities, [position for position in self.get_positions(prices)]))

    def update_equity(self, equity):
        self.equity = equity

    def get_holdings(self):
        return dict(zip(self.securities, self.current_quantities))

    def get_name(self):
        return ','.join(self.securities)

    def get_equity(self):
        return pandas.DataFrame(self.equity_history).set_index('date')

    def get_positions_history(self):
        return pandas.DataFrame(self.positions_history)

    def get_holdings_history(self):
        return pandas.DataFrame(self.holdings_history)

    def get_net_position(self):
        return pandas.DataFrame(self.net_position_history).set_index('date')

    def get_gross_position(self):
        return pandas.DataFrame(self.gross_position_history).set_index('date')

    def get_sharpe_ratio(self):
        mean_return = self.get_equity().pct_change().mean()
        std_return = self.get_equity().pct_change().std()
        value = mean_return / std_return * math.sqrt(250)
        return value['equity']

    def get_drawdown(self):
        cum_returns = (1. + self.get_equity().pct_change()).cumprod()
        return 1. - cum_returns.div(cum_returns.cummax())


class StrategyRunner(object):

    def __init__(self, securities, regression, warmup_period, position_adjuster):
        self.securities = securities
        self.day = None
        self.last_phase = 'BeforeOpen'
        self.regression = regression
        self.warmup_period = warmup_period
        self.count_day = 0
        self.target_quantities = None
        self.position_adjuster = position_adjuster

    def on_open(self, day, prices_open):
        assert self.last_phase == 'BeforeOpen'
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
        signal_values = prices_close_adj
        dependent_price, independent_prices = signal_values[0], signal_values[1:]
        self.regression.compute_regression(dependent_price, independent_prices.tolist())
        deviation = self.regression.get_residual_error()
        self.position_adjuster.update_state(self.day, deviation, signal_values)
        weights = self.regression.get_weights()
        signal = self.regression.get_residual()
        self.target_quantities = self.position_adjuster.update_target_positions(self.day, signal, weights, prices_close)
        self.last_phase = 'BeforeOpen'

    def get_closed_trades(self):
        return pandas.DataFrame(self.position_adjuster.closed_trades)

    def get_open_trades(self):
        return pandas.DataFrame(self.position_adjuster.open_trades)


def process_strategy(securities, regression, warmup_period, prices_by_security,
                     step_size, start_equity, max_net_position, max_gross_position, max_risk_scale):
    """

    :param securities:
    :param regression:
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
    position_adjuster.update_equity(start_equity)
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

    chart_bollinger = list()
    chart_beta = list()
    chart_regression = list()
    strategy_runner = StrategyRunner(securities, regression, warmup_period, position_adjuster)
    for count_day, day in enumerate(sorted(dates)):
        strategy_runner.on_open(day, prices_open[prices_open.index == day].values.transpose()[0])
        strategy_runner.on_close(prices_close[prices_close.index == day].values.transpose()[0])
        strategy_runner.on_after_close(prices_dividend[prices_dividend.index == day].values.transpose()[0],
                                       prices_close_adj[prices_close_adj.index == day].values.transpose()[0],
                                       prices_close[prices_close.index == day].values.transpose()[0]
                                       )
        # statistics
        level_inf = position_adjuster.level_inf()
        level_sup = position_adjuster.level_sup()
        signal_data = {
            'date': strategy_runner.day,
            'level_inf': level_inf,
            'level_sup': level_sup,
            'signal_fls': regression.get_residual()
        }

        chart_bollinger.append(signal_data)

        beta_data = dict()
        for count_factor, weight in enumerate(regression.get_factors()):
            beta_data['beta%d' % count_factor] = weight

        beta_data['date'] = strategy_runner.day
        chart_beta.append(beta_data)

        regression_data = {'date': strategy_runner.day,
                           'y*': regression.get_estimate(),
                           'portfolio': regression.get_estimate() - regression.get_factors()[-1]
                           }
        chart_regression.append(regression_data)

    closed_trades = strategy_runner.get_closed_trades()
    mean_trade = closed_trades['pnl'].mean()
    worst_trade = closed_trades['pnl'].min()
    count_trades = closed_trades['pnl'].count()
    max_drawdown = position_adjuster.get_drawdown().max()['equity']
    final_equity = position_adjuster.get_equity()['equity'][-1]
    summary = {
        'sharpe_ratio': position_adjuster.get_sharpe_ratio(),
        'average_trade': mean_trade,
        'worst_trade': worst_trade,
        'count_trades': count_trades,
        'max_drawdown_pct': max_drawdown,
        'final_equity': final_equity
    }
    logging.info('result: %s' % str(summary))
    result = {
        'summary': summary,
        'bollinger': pandas.DataFrame(chart_bollinger).set_index('date'),
        'factors': pandas.DataFrame(chart_beta).set_index('date'),
        'regression': chart_regression,
        'equity': position_adjuster.get_equity(),
        'net_position': position_adjuster.get_net_position(),
        'gross_position': position_adjuster.get_gross_position(),
        'positions': position_adjuster.get_positions_history(),
        'holdings': pandas.DataFrame(position_adjuster.get_holdings_history()),
        'fills': position_adjuster.get_fills(),
        'next_target_quantities': strategy_runner.target_quantities
    }
    return result


def backtest_portfolio(start_date, end_date, symbols, prices_path, lookback_period,
                       step_size, start_equity, max_net_position, max_gross_position,
                       max_risk_scale):
    securities = ['PCX/' + symbol for symbol in symbols]
    prices_by_security = dict()
    close_prices = pandas.DataFrame()
    max_start_date = start_date
    min_end_date = end_date
    for security in securities:
        exchange, security_code = security.split('/')
        prices_df = load_prices(prices_path, exchange, security_code)
        prices_by_security[security] = prices_df
        if max_start_date is not None:
            max_start_date = max(max_start_date, prices_df.index.min())

        else:
            max_start_date = prices_df.index.min()

        if min_end_date is not None:
            min_end_date = min(min_end_date, prices_df.index.max())

        else:
            min_end_date = prices_df.index.max()

        close_prices[security] = prices_df['close adj']

    close_prices.reset_index(inplace=True)
    logging.info('considering date range: %s through %s' % (max_start_date, min_end_date))

    for security in securities:
        prices_by_security[security] = prices_by_security[security][
            (prices_by_security[security].index >= max_start_date) & (prices_by_security[security].index <= min_end_date)]

    warmup_period = 10

    #regression0 = RegressionModelFLS(securities, delta=5E-6, with_constant_term=False)
    regression10 = RegressionModelOLS(securities, with_constant_term=False, lookback_period=lookback_period)
    backtest_data = process_strategy(securities, regression10, warmup_period, prices_by_security,
                                     step_size=step_size, start_equity=start_equity,
                                     max_net_position=max_net_position,
                                     max_gross_position=max_gross_position,
                                     max_risk_scale=max_risk_scale)
    backtest_summary = backtest_data['summary']
    backtest_summary['portfolio'] = '/'.join(symbols)
    return backtest_data


def chart_backtest(start_date, end_date, securities, prices_path, lookback_period,
                   step_size, start_equity,
                   max_net_position, max_gross_position, max_risk_scale):
    pyplot.style.use('ggplot')
    backtest_result = backtest_portfolio(start_date, end_date, securities, prices_path, lookback_period=lookback_period,
                                         step_size=step_size, start_equity=start_equity,
                                         max_net_position=max_net_position,
                                         max_gross_position=max_gross_position,
                                         max_risk_scale=max_risk_scale)
    backtest_result['equity'].plot()
    backtest_result['net_position'].plot()
    backtest_result['gross_position'].plot()
    pyplot.gca().get_yaxis().get_major_formatter().set_useOffset(False)
    backtest_result['factors'].plot(subplots=True)
    backtest_result['bollinger'].plot(subplots=False)
    pyplot.show()


def main(args):
    # TODO arg line
    prices_path = os.sep.join(['..', 'data', 'eod'])
    start_date = date(int(args.start_yyyymmdd[:4]), int(args.start_yyyymmdd[4:6]), int(args.start_yyyymmdd[6:8]))
    end_date = date(int(args.end_yyyymmdd[:4]), int(args.end_yyyymmdd[4:6]), int(args.end_yyyymmdd[6:8]))
    if args.display is not None:
        securities = args.display.split('/')
        chart_backtest(start_date, end_date, securities, prices_path, lookback_period=args.lookback_period,
                       step_size=args.step_size, start_equity=args.starting_equity,
                       max_net_position=args.max_net_position,
                       max_gross_position=args.max_gross_position,
                       max_risk_scale=args.max_risk_scale)

    elif args.display_portfolio is not None:
        pyplot.style.use('ggplot')
        backtest_results = dict()
        positions = pandas.DataFrame()
        holdings = pandas.DataFrame()
        fills = pandas.DataFrame()
        target_quantities = list()
        with open(args.display_portfolio) as portfolio_file:
            portfolios = [line.strip().split(',') for line in portfolio_file.readlines() if len(line.strip()) > 0]
            logging.info('loaded portfolios: %s' % portfolios)
            for lookback_period, portfolio in portfolios:
                securities = portfolio.split('/')
                backtest_result = backtest_portfolio(start_date, end_date, securities, prices_path, lookback_period=int(lookback_period),
                                                     step_size=args.step_size, start_equity=args.starting_equity,
                                                     max_net_position=args.max_net_position,
                                                     max_gross_position=args.max_gross_position,
                                                     max_risk_scale=args.max_risk_scale)
                positions = pandas.concat([positions, backtest_result['positions']])
                holdings = pandas.concat([holdings, backtest_result['holdings']])
                fills = pandas.concat([fills, backtest_result['fills']])
                if backtest_result['next_target_quantities'] is not None:
                    yahoo_codes = ['PCX/' + code for code in securities]
                    target_quantities += zip(yahoo_codes, backtest_result['next_target_quantities'])

                if 'equity' not in backtest_results:
                    backtest_results['equity'] = backtest_result['equity']

                else:
                    backtest_results['equity'] += backtest_result['equity']

        latest_holdings = holdings.pivot_table(index='date', columns='security', values='quantity', aggfunc=numpy.sum).tail(1).transpose()
        latest_holdings.columns = ['quantity']
        logging.info('stocks:\n%s' % latest_holdings)
        target_df = pandas.DataFrame(dict(target_quantities), index=[0]).transpose()
        target_df.columns=['target']
        logging.info('new target quantities:\n%s' % target_df)
        logging.info('trades:\n%s' % (target_df['target'] - latest_holdings['quantity']).dropna())

        equity = backtest_results['equity']
        equity.plot()

        by_security_pos = positions.pivot_table(index='date', columns='security', values='position', aggfunc=numpy.sum)
        by_security_pos.plot()

        positions_aggregated_net = positions.groupby('date')['position'].sum()
        positions_aggregated_gross = positions.groupby('date')['position'].agg(lambda x: numpy.abs(x).sum())
        positions_aggregated = pandas.DataFrame(index=positions_aggregated_net.index,
                                                data=numpy.array([positions_aggregated_net, positions_aggregated_gross]).transpose(),
                                                columns=['net', 'gross'])
        positions_aggregated['margin_warning'] = equity / 0.4
        positions_aggregated.plot(subplots=False)

        days_interval = (equity.index[-1] - equity.index[0])
        starting_equity = equity.dropna().head(1)['equity'].values[0]
        ending_equity = equity.dropna().tail(1)['equity'].values[0]
        sharpe_ratio = equity.dropna().pct_change().mean() / equity.dropna().pct_change().std() * math.sqrt(250)
        logging.info('sharpe ratio: %.2f', sharpe_ratio)
        annualized_return = 100 * (numpy.power(ending_equity / starting_equity, 365 / days_interval.days) - 1)
        logging.info('annualized return: %.2f percent' % annualized_return)
        logging.info('fills:\n%s', fills.sort_values('date').set_index(['date', 'security']))
        pyplot.show()

    else:
        # backtest batch
        # TODO arg line
        portfolios_path = os.sep.join(['..', 'data', 'portfolios.csv'])
        with open(portfolios_path) as portfolios_file:
            portfolios = [line.strip().split(',') for line in portfolios_file.readlines()]
            results = list()
            for symbols in portfolios:
                result = backtest_portfolio(start_date, end_date, symbols, prices_path, lookback_period=args.lookback_period,
                                            step_size=args.step_size, start_equity=args.starting_equity,
                                            max_net_position=args.max_net_position,
                                            max_gross_position=args.max_gross_position,
                                            max_risk_scale=args.max_risk_scale)

                results.append(result['summary'])

            result_df = pandas.DataFrame(results).set_index('portfolio')
            result_df.to_csv('backtest-results.csv')
            print(result_df)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('backtest.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='Backtesting prototype.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--start-yyyymmdd', type=str, help='backtest start date', default='20130101')
    parser.add_argument('--end-yyyymmdd', type=str, help='backtest end date', default=date.today().strftime('%Y%m%d'))
    parser.add_argument('--display', type=str, help='display portfolio made of comma-separated securities')
    parser.add_argument('--display-portfolio', type=str, help='display aggregated portfolio from specified file')
    parser.add_argument('--lookback-period', type=int, help='lookback period', default=200)
    parser.add_argument('--step-size', type=int, help='deviation unit measured in number of standard deviations', default=2)
    parser.add_argument('--starting-equity', type=float, help='deviation unit measured in number of standard deviations', default=20000)
    parser.add_argument('--max-net-position', type=float, help='max allowed net position for one step, measured as a fraction of equity', default=0.4)
    parser.add_argument('--max-gross-position', type=float, help='max allowed gross position for one step, measured as a fraction of equity', default=1.2)
    parser.add_argument('--max-risk-scale', type=int, help='max number of steps', default=3)
    args = parser.parse_args()
    main(args)
