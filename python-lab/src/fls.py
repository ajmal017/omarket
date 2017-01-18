import math
import numpy


class FlexibleLeastSquare(object):

    def __init__(self, size, delta, v_epsilon):
        self.beta = numpy.matrix([0.] * (size + 1)).transpose()
        self.cov_beta = numpy.matrix([[0.] * (size + 1)] * (size + 1))
        self.v_epsilon = v_epsilon
        self.v_omega = numpy.identity(size + 1) * delta / (1. - delta)
        self.first_round = True

    class Result(object):
        def __init__(self, estimated_output, beta, var_output_error):
            self.estimated_output = estimated_output
            self.beta = [value[0] for value in beta.tolist()]
            self.var_output_error = var_output_error

    def estimate(self, output_value, inputs):
        """

        :param output: scalar value of measured output
        :param inputs: list of measured input values
        :return:
        """
        cov_beta_prediction = self.cov_beta + self.v_omega
        factors = numpy.matrix(inputs + [1.]).transpose()
        output = numpy.matrix([output_value])
        output_estimated = factors.transpose() * self.beta
        var_output_error = factors.transpose()* cov_beta_prediction * factors + self.v_epsilon
        output_error = output - output_estimated
        kalman_gain = cov_beta_prediction * factors / var_output_error
        self.beta += kalman_gain * output_error
        self.cov_beta = cov_beta_prediction - kalman_gain * factors.transpose() * cov_beta_prediction
        if self.first_round:
            self.first_round = False
            result = FlexibleLeastSquare.Result(output_value, self.beta, math.sqrt(var_output_error))

        else:
            result = FlexibleLeastSquare.Result(output_estimated.item(), self.beta, math.sqrt(var_output_error))

        return result
