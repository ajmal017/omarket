import unittest
import numpy
import pandas
import math

from discretize import discretize, HysteresisDiscretize


class DiscretizeTest(unittest.TestCase):

    def test_simple(self):
        inputs = numpy.linspace(-1, 1, 10000)
        df = pandas.DataFrame(inputs)
        df['output1'] = df.apply(lambda row: discretize(row[0], 0.5, 0.1), axis=1)
        df['output2'] = df.apply(lambda row: discretize(row[0], 0.5, -0.1), axis=1)
        diff = (df['output1'] - df['output2']).diff()
        diff = diff[diff != 0.]
        expected = numpy.array([numpy.nan, 0.5, -0.5,  0.5, -0.5,  0.5, -0.5,  0.5, -0.5])
        numpy.testing.assert_array_almost_equal(expected, diff.values, decimal=6)

    def test_hysteresis_linear(self):
        inputs = numpy.concatenate([numpy.linspace(-1., 1., 10000), numpy.linspace(1., -1., 10000)])
        df = pandas.DataFrame(inputs, columns=['input'])
        df['output1'] = df.apply(lambda row: discretize(row[0], 0.5, 0.1), axis=1)
        df['output2'] = df.apply(lambda row: discretize(row[0], 0.5, -0.1), axis=1)
        hyst = HysteresisDiscretize(0.5, ratio=0.2)
        df['hyst'] = df.apply(lambda row: hyst(row[0]), axis=1)
        part1 = df[df.index < 10000]
        part2 = df[df['input'] >= 10000]
        a = part1[part1['hyst'].notnull()]['output1']
        b = part1[part1['hyst'].notnull()]['hyst']
        c = part2['output2']
        d = part2['hyst']
        numpy.testing.assert_array_almost_equal(a, b, decimal=6)
        numpy.testing.assert_array_almost_equal(c, d, decimal=6)

    def test_hysteresis_sine(self):
        basis = numpy.linspace(-2. * math.pi, 2. * math.pi, 10000)
        inputs = 1.5 * numpy.sin(basis) + 0.15 * numpy.sin(basis * 100)
        df = pandas.DataFrame(inputs, columns=['input'])
        df['output1'] = df.apply(lambda row: discretize(row[0], 0.5, 0.1), axis=1)
        df['output2'] = df.apply(lambda row: discretize(row[0], 0.5, -0.1), axis=1)
        hyst = HysteresisDiscretize(0.5, ratio=0.3)
        df['hyst'] = df.apply(lambda row: hyst(row[0]), axis=1)
        diff = df['hyst'].diff()
        diff = diff[diff != 0.]
        self.assertEqual(diff.dropna().shape, (19, ))

if __name__ == '__main__':
    unittest.main()
