import itertools
import random
from decimal import Decimal

import math
from typing import List, Iterable

import os


def random_walk(initial: Decimal=0, step: Decimal=1, seed_value: float=1) -> Iterable[Decimal]:
    random.seed(seed_value)
    value = initial
    while True:
        movement = -1 if random.random() < Decimal('0.5') else 1
        value += step * Decimal(movement)
        yield value


def random_walk_with_floor(floor: Decimal, initial: Decimal=0, step: Decimal=1, seed_value: float=3) -> Iterable[Decimal]:
    for value in random_walk(initial=initial, step=step, seed_value=seed_value):
        if value <= floor:
            break

        yield value

    while True:
        yield floor


def poisson_probability(lambda_value: float, k_value: int) -> float:
    """

    :param lambda_value: non-zero, positive parameter (expected value)
    :param k_value: positive rank
    :return:
    """
    return math.exp(-lambda_value) * (lambda_value ** k_value) / math.factorial(k_value)


def poisson_event(lambda_value: float, k_value: int) -> bool:
    while True:
        yield random.random() < poisson_probability(lambda_value, k_value)


def discretize(value: Decimal, step: Decimal) -> Decimal:
    """
    >>> discretize(Decimal('100.015'), Decimal('0.05'))
    Decimal('100.00')
    >>> discretize(Decimal('100.601'), Decimal('0.05'))
    Decimal('100.60')
    >>> discretize(Decimal('100.661'), Decimal('0.05'))
    Decimal('100.65')
    >>> discretize(Decimal('100.689'), Decimal('0.05'))
    Decimal('100.70')

    :param value: input value
    :param step: making ouput multiple of step
    :return:
    """
    return step * Decimal(int(round(value / step)))


def prices_sequence(initial_price: Decimal, tick_size: Decimal, depth: int, ascending: bool=False) -> List[Decimal]:
    starting_price = discretize(initial_price, tick_size)
    if ascending:
        if starting_price < initial_price:
            price_range = range(1, depth + 1)

        else:
            price_range = range(0, depth)

        return [Decimal(starting_price + increment * tick_size) for increment in price_range]

    else:
        if starting_price > initial_price:
            price_range = range(1, depth + 1)

        else:
            price_range = range(0, depth)

        return [Decimal(starting_price - decrement * tick_size) for decrement in price_range]


class OrderBook(object):

    def __init__(self, initial_price, tick_size, depth=10):
        bid_prices = prices_sequence(initial_price, tick_size, depth, ascending=False)
        ask_prices = prices_sequence(initial_price, tick_size, depth, ascending=True)
        self._bids = [(price, 0) for price in bid_prices]
        self._asks = [(price, 0) for price in ask_prices]

    def update(self, price, bids, asks):
        for rank, bid in enumerate(bids):
            if bid:
                price, volume = self._bids[rank]
                self._bids[rank] = (price, volume + 1)

        for rank, ask in enumerate(asks):
            if ask:
                price, volume = self._asks[rank]
                self._asks[rank] = (price, volume + 1)

    @property
    def bids(self):
        return self._bids

    @property
    def asks(self):
        return self._asks

    def __repr__(self):
        book = ''
        for bid, ask in zip(self.bids, self.asks):
            book += '%s | %s' % (bid, ask) + os.linesep

        return book


def orderbook_update(depth=10):
    rate = 2
    bid_levels = [poisson_event(rate, level) for level in range(depth)]
    ask_levels = [poisson_event(rate, level) for level in range(depth)]
    return zip(zip(*bid_levels), zip(*ask_levels))


def main():
    tick_size = Decimal('0.01')
    step_size = tick_size * Decimal('0.1')

    price = Decimal('100.642')
    depth = 10
    orderbook = OrderBook(initial_price=price, tick_size=tick_size, depth=depth)
    print(orderbook)
    for bids, asks in itertools.islice(orderbook_update(depth=depth), 30):
        orderbook.update(price, bids, asks)
        print(orderbook)


if __name__ == '__main__':
    main()

