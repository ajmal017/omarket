import argparse
import logging
import pandas
import random
import math
from matplotlib import pyplot
import numpy


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


class FlexibleLeastSquare(object):

    def __init__(self, size, delta, v_epsilon):
        self.beta = numpy.matrix([0.] * (size + 1)).transpose()
        self.p = numpy.matrix([[0.] * (size + 1)] * (size + 1))
        self.v_epsilon = v_epsilon
        self.v_w = numpy.identity(size + 1) * delta / (1. - delta)

    def estimate(self, output, inputs):
        """

        :param output: scalar value of measured output
        :param inputs: list of measured input values
        :return:
        """
        var_state_error = self.p + self.v_w
        factors = numpy.matrix(inputs + [1.]).transpose()
        output = numpy.matrix([output])
        output_estimated = factors.transpose() * self.beta
        var_output_error = factors.transpose()* var_state_error * factors + self.v_epsilon
        error = output - output_estimated
        kalman_gain = var_state_error * factors / var_output_error
        self.beta += kalman_gain * error
        self.p = var_state_error - kalman_gain * factors.transpose() * var_state_error
        return output_estimated.item(), self.beta, math.sqrt(var_output_error)


def main(args):
    pyplot.style.use('ggplot')
    states = list()
    walk1 = value1()
    walk2 = value2()
    walk3 = value3()
    v_epsilon = 0.0001
    delta = 0.001
    fls = FlexibleLeastSquare(3, delta, v_epsilon)
    for count in range(1000):
        v1 = next(walk1)
        v2 = next(walk2)
        v3 = next(walk3)
        epsilon = noise()
        y = combine(v1, v2, v3, epsilon)

        # FLS
        output_estimated, beta, var = fls.estimate(y, [v1, v2, v3])
        logging.info('beta:\n%s' % beta)
        logging.info('var: %s' % var)
        state = {'y': y, 'signal': output_estimated, 'v1': v1, 'v2': v2, 'v3': v3, 'noise': epsilon}
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
