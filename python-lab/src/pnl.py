import logging
import math


class AverageCostProfitAndLoss(object):
    """
    Computes P&L based on weighted average cost method.
    """

    def __init__(self, quantity=0, cost=0., realized_pnl=0):
        self._quantity = quantity
        self._cost = cost
        self._realized_pnl = realized_pnl

    @property
    def realized_pnl(self):
        return self._realized_pnl

    @property
    def cost(self):
        return self._cost

    @property
    def quantity(self):
        return self._quantity

    @property
    def average_price(self):
        return self._cost / self._quantity

    def get_market_value(self, current_price):
        return self.quantity * current_price

    def get_unrealized_pnl(self, current_price):
        return self.get_market_value(current_price) - self.cost

    def get_total_pnl(self, current_price):
        return self.realized_pnl + self.get_unrealized_pnl(current_price)

    def add_fill(self, fill_qty, fill_price, fees=None):
        """
        Adding a fill to the record updates the P&L values.

        :param fill_qty:
        :param fill_price:
        :param fees: a dict containing fees that apply on the trade
        :return:
        """
        logging.info('adding fill: %s at %s', fill_qty, fill_price)
        old_qty = self._quantity
        old_cost = self._cost
        old_realized = self._realized_pnl
        if old_qty == 0:
            self._quantity = fill_qty
            self._cost = fill_qty * fill_price

        else:
            closing_qty = 0
            opening_qty = fill_qty
            if math.copysign(1, old_qty) != math.copysign(1, fill_qty):
                closing_qty = min(abs(old_qty), abs(fill_qty)) * math.copysign(1, fill_qty)
                opening_qty = fill_qty - closing_qty

            self._quantity = old_qty + fill_qty
            self._cost = old_cost + (opening_qty * fill_price) + (closing_qty * old_cost / old_qty)
            self._realized_pnl = old_realized + closing_qty * (old_cost / old_qty - fill_price)
