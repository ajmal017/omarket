import argparse
import logging
import random

import pandas
from matplotlib import pyplot

from fls import DynamicLinearRegression


def random_walk(start_value, amplitude):
    random.seed(-1)
    new_value = start_value
    while True:
        delta = random.choice([-1., 1.]) * amplitude
        new_value += delta
        yield new_value


def combine(dep1, dep2, dep3, noise):
    return 0.5 * dep1 - 0.1 * dep2 + 5. * dep3 + 0.5 * noise


def value1():
    return random_walk(-0.2, 0.01)


def value2():
    return random_walk(-0.2, 0.01)


def value3():
    return random_walk(-0.2, 0.01)


def noise():
    return random.normalvariate(mu=0, sigma=0.2)


def main(args):
    pyplot.style.use('ggplot')
    states = list()
    walk1 = value1()
    walk2 = value2()
    walk3 = value3()
    v_epsilon = 0.0001
    delta = 0.001
    fls = DynamicLinearRegression(4, delta, v_epsilon)
    for count in range(1000):
        v1 = next(walk1)
        v2 = next(walk2)
        v3 = next(walk3)
        epsilon = noise()
        y = combine(v1, v2, v3, epsilon)

        # FLS
        result = fls.estimate(y, [v1, v2, v3, 1.])
        logging.info('beta:\n%s' % result.beta)
        logging.info('var: %s' % result.var_output_error)
        state = {'y': y, 'signal': result.estimated_output, 'v1': v1, 'v2': v2, 'v3': v3, 'noise': epsilon}
        states.append(state)

    df = pandas.DataFrame(states)
    df.plot()
    pyplot.show()

    logging.info('testing fls')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('draft.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('starting script')
    parser = argparse.ArgumentParser(description='draft script.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    args = parser.parse_args()
    main(args)
