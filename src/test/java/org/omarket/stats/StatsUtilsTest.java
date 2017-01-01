package org.omarket.stats;

import joinery.DataFrame;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixChangingVisitor;
import org.apache.commons.math3.util.Precision;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.omarket.stats.CoIntegration.cointegration_johansen;

/**
 * Created by christophe on 30/12/16.
 */
public class StatsUtilsTest {
    @Test
    public void testDetrend() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 11., 21.},
                {2., 12., 22.},
                {3., 13., 23.},
                {4., 14., 24.},
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        double[][] expectedColumns = {
                {-2.0, -2.0, -2.0},
                {-1.0, -1.0, -1.0},
                {0.0, 0.0, 0.0},
                {1.0, 1.0, 1.0},
                {2.0, 2.0, 2.0}};
        assertEquals(MatrixUtils.createRealMatrix(expectedColumns), StatsUtils.constantDetrendColumns(matrix));

        double[][] expectedRows = {
                {-10.0, 0.0, 10.0},
                {-10.0, 0.0, 10.0},
                {-10.0, 0.0, 10.0},
                {-10.0, 0.0, 10.0},
                {-10.0, 0.0, 10.0}};
        assertEquals(MatrixUtils.createRealMatrix(expectedRows), StatsUtils.constantDetrendRows(matrix));
    }

    @Test
    public void testRowDiff() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.},
                {5., 30., 10.},
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        double[][] expectedRows = {
                {0.0, 0.0, 0.0},
                {1.0, 2.0, -2.0},
                {1.0, 3.0, -2.0},
                {1.0, 4.0, -2.0},
                {1.0, 5.0, -2.0},
                {1.0, 6.0, -2.0}};
        assertEquals(MatrixUtils.createRealMatrix(expectedRows), StatsUtils.diffRows(matrix));
    }

    @Test
    public void testShitDown() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.},
                {5., 30., 10.},
        };
        double[][] expectedRows = {
                {0., 0., 0.},
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.}
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        assertEquals(MatrixUtils.createRealMatrix(expectedRows), StatsUtils.shiftDown(matrix));
    }

    @Test
    public void testRowDiffTruncate() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.},
                {5., 30., 10.},
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        double[][] expectedRows = {
                {1.0, 2.0, -2.0},
                {1.0, 3.0, -2.0},
                {1.0, 4.0, -2.0},
                {1.0, 5.0, -2.0},
                {1.0, 6.0, -2.0}};
        assertEquals(MatrixUtils.createRealMatrix(expectedRows), StatsUtils.truncateTop(StatsUtils.diffRows(matrix)));
    }

    @Test
    public void testTruncate() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.},
                {5., 30., 10.},
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        double[][] expectedRows = {
                {4., 24., 12.},
                {5., 30., 10.}
        };
        assertEquals(MatrixUtils.createRealMatrix(expectedRows), StatsUtils.truncateTop(matrix, 4));
        assertEquals(matrix, StatsUtils.truncateTop(matrix, 0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTruncateOverflow() throws Exception {
        double[][] data = {
                {0., 10., 20.},
                {1., 12., 18.},
                {2., 15., 16.},
                {3., 19., 14.},
                {4., 24., 12.},
                {5., 30., 10.},
        };
        RealMatrix matrix = MatrixUtils.createRealMatrix(data);
        StatsUtils.truncateTop(matrix, 6);
    }

    @Test
    public void testJohansen() throws Exception {
        Path csvPath = Paths.get(ClassLoader.getSystemResource("test-johansen.csv").toURI());
        DataFrame df = DataFrame.readCsv(Files.newInputStream(csvPath));
        List<double[]> rows = new LinkedList<>();
        for (ListIterator row = df.iterrows(); row.hasNext(); ) {
            List<Double> fields = (List<Double>) row.next();
            rows.add(fields.stream().mapToDouble(Double::doubleValue).toArray());
        }
        double[][] values = new double[rows.size()][];
        for (int i = 0; i < rows.size(); i++) {
            values[i] = rows.get(i);
        }
        RealMatrix matrix = MatrixUtils.createRealMatrix(values);
        CoIntegration.Result result = cointegration_johansen(matrix);
        // TODO
    }

    @Test
    public void testJohansenShort() throws Exception {
        Path csvPath = Paths.get(ClassLoader.getSystemResource("test-johansen.csv").toURI());
        DataFrame df = DataFrame.readCsv(Files.newInputStream(csvPath));
        List<double[]> rows = new LinkedList<>();
        int count = 20;
        for (ListIterator row = df.iterrows(); row.hasNext(); ) {
            if (count == 0) {
                break;
            }
            List<Double> fields = (List<Double>) row.next();
            rows.add(fields.stream().mapToDouble(Double::doubleValue).toArray());
            count--;
        }
        double[][] values = new double[rows.size()][];
        for (int i = 0; i < rows.size(); i++) {
            values[i] = rows.get(i);
        }
        RealMatrix matrix = MatrixUtils.createRealMatrix(values);
        CoIntegration.Result result = cointegration_johansen(matrix);

        assertArrayEquals(new double[]{0.452506, 0.187277, 0.042555}, result.getEigenValues().toArray(), 1E-5);
        double[][] dataEVec = new double[][]{
                {0.49543, 0.36112, 1.98268},
                {0.57683, -2.69169, -3.25477},
                {0.13325, 3.10763, 1.91643}
        };
        RealMatrix expectedEVec = MatrixUtils.createRealMatrix(dataEVec);
        MatrixComparator evecCompare = new MatrixComparator(expectedEVec, 5);
        result.getEigenVectors().walkInColumnOrder(evecCompare);
        assertTrue(evecCompare.result(), evecCompare.areEqual());
        assertArrayEquals(new double[]{15.35861, 4.51534, 0.78276}, result.getLr1().toArray(), 1E-5);
        assertArrayEquals(new double[]{10.84328, 3.73257, 0.78276}, result.getLr2().toArray(), 1E-5);

        double[][] dataCVT = new double[][]{
                {27.0669, 29.7961, 35.4628},
                {13.4294, 15.4943, 19.9349},
                {2.7055, 3.8415, 6.6349}
        };
        RealMatrix expectedCVT = MatrixUtils.createRealMatrix(dataCVT);
        MatrixComparator cvtCompare = new MatrixComparator(expectedCVT, 5);
        result.getCvt().walkInColumnOrder(cvtCompare);
        assertTrue(cvtCompare.result(), cvtCompare.areEqual());
        double[][] dataCVM = new double[][]{
                {18.8928, 21.1314, 25.8650},
                {12.2971, 14.2639, 18.5200},
                {2.7055, 3.8415, 6.6349}
        };
        RealMatrix expectedCVM = MatrixUtils.createRealMatrix(dataCVM);
        MatrixComparator cvmCompare = new MatrixComparator(expectedCVM, 5);
        result.getCvm().walkInColumnOrder(cvmCompare);
        assertTrue(cvmCompare.result(), cvmCompare.areEqual());
    }

    private static class MatrixComparator implements RealMatrixChangingVisitor {
        private final RealMatrix other;
        private final int precisionDecimals;
        boolean areEqual = true;
        double actual;
        double expected;
        int diffRow;
        int diffColumn;

        MatrixComparator(RealMatrix other, int precisionDecimals) {
            this.other = other;
            this.precisionDecimals = precisionDecimals;
        }

        @Override
        public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
        }

        @Override
        public double visit(int row, int column, double value) {
            if (!areEqual) {
                return 0;
            }
            double otherValue = other.getEntry(row, column);
            if (!Precision.equals(value, otherValue, Math.pow(10, -precisionDecimals))) {
                areEqual = false;
                actual = otherValue;
                expected = value;
                diffRow = row;
                diffColumn = column;
            }
            return 0;
        }

        @Override
        public double end() {
            return 0;
        }

        boolean areEqual() {
            return areEqual;
        }

        String result() {
            String precisionFormat = "%." + precisionDecimals + "f";
            return "difference detected: (" + diffRow + "," + diffColumn + ") = " + String.format(precisionFormat, actual) + " vs. " + String.format(precisionFormat, expected);
        }
    }
}
