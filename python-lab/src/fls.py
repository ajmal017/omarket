import math

import numpy


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