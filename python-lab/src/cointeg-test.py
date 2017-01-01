import unittest
import os
import sys

import numpy
import pandas

import cointeg


class TestCointegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_module = sys.modules[__name__]
        cls._resources_path = os.sep.join(
            [os.path.dirname(current_module.__file__), '..', '..', 'src', 'test', 'resources'])

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_johansen(self):
        resource_path = os.sep.join([self._resources_path, 'test-johansen.csv'])
        resource_path_norm = os.path.abspath(resource_path)
        y = pandas.read_csv(resource_path_norm)
        vectors = cointeg.get_johansen(y, lag=1)
        v1 = vectors[0]
        v2 = vectors[1]
        expected_v1 = numpy.array([1., -1.9999231, 2.6499922])
        numpy.testing.assert_almost_equal(v1, expected_v1)
        expected_v2 = numpy.array([-2.3183438, 4.6361944, -1.])
        numpy.testing.assert_almost_equal(v2, expected_v2)
        self.assertFalse(cointeg.is_not_stationary(numpy.dot(y.values, v1), significance='10%'))
        self.assertFalse(cointeg.is_not_stationary(numpy.dot(y.values, v2), significance='10%'))


if __name__ == '__main__':
    unittest.main()
