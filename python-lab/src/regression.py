import logging
import math
import pandas
import numpy
from statsmodels.formula.api import OLS
from statsmodels.tools import add_constant

from fls import FlexibleLeastSquare

_LOGGER = logging.getLogger('regression')


class RegressionModelFLS(object):

    def __init__(self, securities, delta, with_constant_term=True):
        self.with_constant_term = with_constant_term
        size = len(securities) - 1
        if self.with_constant_term:
            size += 1

        initial_state_mean = numpy.zeros(size)
        initial_state_covariance = numpy.ones((size, size))
        observation_covariance = 5E-5
        trans_cov = delta / (1. - delta) * numpy.eye(size)

        self.result = None
        self.fls = FlexibleLeastSquare(initial_state_mean, initial_state_covariance, observation_covariance, trans_cov)

    def compute_regression(self, y_value, x_values):
        independent_values = x_values
        if self.with_constant_term:
            independent_values += [1.]

        self.result = self.fls.estimate(y_value, independent_values)

    def get_residual_error(self):
        return math.sqrt(self.result.var_output_error)

    def get_factors(self):
        return self.result.beta

    def get_estimate(self):
        return self.result.estimated_output

    def get_weights(self):
        weights = self.get_factors()
        if self.with_constant_term:
            weights = weights[:-1]

        return numpy.array([-1.] + weights)

    def get_residual(self):
        return self.result.error


class RegressionModelOLS(object):

    def __init__(self, securities, with_constant_term=True, lookback_period=200):
        self._lookback = lookback_period
        self.current_x = None
        self.current_y = None
        self._with_constant_term= with_constant_term
        self._counter = 0
        self._y_values = list()
        self._x_values = [list() for item in securities[1:]]
        self.securities = securities
        self.result = None

    def add_samples(self, y_value, x_values):
        self._counter += 1
        self._y_values.append(y_value)
        if self._counter > self._lookback:
            self._y_values.pop(0)
            for target_lists in self._x_values:
                target_lists.pop(0)

        for target_list, new_item in zip(self._x_values, x_values):
            target_list.append(new_item)

        independent_values = x_values
        if self._with_constant_term:
            independent_values += [1.]

        self.current_x = independent_values
        self.current_y = y_value

    def compute_regression(self):
        if len(self._y_values) < len(self.securities) - 1:
            # not enough values for regression
            _LOGGER.error('not enough values for regression')

        dependent = pandas.DataFrame({self.securities[0]: self._y_values})
        independent = pandas.DataFrame({key: self._x_values[count] for count, key in enumerate(self.securities[1:])})
        if self._with_constant_term:
            ols = OLS(dependent, add_constant(independent))

        else:
            ols = OLS(dependent, independent)

        self.result = ols.fit()

    def get_residual_error(self):
        """
        Standard error of estimates.
        :return:
        """
        return math.sqrt(self.result.mse_resid)

    def get_factors(self):
        if self._with_constant_term:
            return self.result.params[self.securities[1:] + ['const']]

        else:
            return self.result.params[self.securities[1:]]

    def get_estimate(self):
        if self._with_constant_term:
            return self.result.params[self.securities[1:] + ['const']].dot(self.current_x)

        else:
            return self.result.params[self.securities[1:]].dot(self.current_x)

    def get_weights(self):
        return numpy.array([-1.] + self.result.params[self.securities[1:]].tolist())

    def get_residual(self):
        return self.current_y - self.get_estimate()

