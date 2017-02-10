import math


class HysteresisDiscretize(object):
    """
    remembers self.n
     __call__() allows the class instance to be called as a function
    """
    def __init__(self, step, ratio):
        self.step = step
        self.ratio = ratio
        self.previous_output = None

    def __call__(self, value):
        if value is None or math.isnan(value):
            return math.nan

        output1 = discretize(value, self.step, shift=self.step * self.ratio)
        output2 = discretize(value, self.step, shift=-self.step * self.ratio)
        output = math.nan
        if output1 == output2:
            output = output1

        elif self.previous_output is not None:
            if self.previous_output == output1:
                output = output1

            elif self.previous_output == output2:
                output = output2

        self.previous_output = output
        return output


def discretize(value, step, shift=0.):
    adj_step = 2. * step
    if value is None or math.isnan(value):
        return math.nan

    adj_value = value - shift
    return int(adj_value * (1. / adj_step)) * adj_step + 0.5 * adj_step * (-1., 1.)[adj_value > 0]
