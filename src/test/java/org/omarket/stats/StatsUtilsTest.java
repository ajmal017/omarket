package org.omarket.stats;

import joinery.DataFrame;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;

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
                {0.0,0.0,0.0},
                {1.0,2.0,-2.0},
                {1.0,3.0,-2.0},
                {1.0,4.0,-2.0},
                {1.0,5.0,-2.0},
                {1.0,6.0,-2.0}};
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
                {1.0,2.0,-2.0},
                {1.0,3.0,-2.0},
                {1.0,4.0,-2.0},
                {1.0,5.0,-2.0},
                {1.0,6.0,-2.0}};
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
    }

    @Test
    public void testJohansen() throws Exception {
        Path csvPath = Paths.get(ClassLoader.getSystemResource("test-johansen.csv").toURI());
        DataFrame df = DataFrame.readCsv(Files.newInputStream(csvPath));
        List<double[]> rows = new LinkedList<>();
        for(ListIterator row = df.iterrows(); row.hasNext();){
            List<Double> fields = (List<Double>) row.next();
            rows.add(fields.stream().mapToDouble(Double::doubleValue).toArray());
        }
        double[][] values = new double[rows.size()][];
        for(int i=0; i< rows.size(); i++){
            values[i] = rows.get(i);
        }
        RealMatrix matrix = MatrixUtils.createRealMatrix(values);
        cointegration_johansen(matrix);
        /*
        eigenvalues=[  2.12612657e-04   3.37863583e-01   3.27530270e-01]
eigenvectors=[
[ 0.0030233   0.28840906 -0.43915418]
[ 0.71405179 -0.57679596  0.87821493]
[-0.70008636  0.76428178 -0.18942582]
            ]
         */
    }

    @Test
    public void testJohansenShort() throws Exception {
        Path csvPath = Paths.get(ClassLoader.getSystemResource("test-johansen.csv").toURI());
        DataFrame df = DataFrame.readCsv(Files.newInputStream(csvPath));
        List<double[]> rows = new LinkedList<>();
        int count = 20;
        for(ListIterator row = df.iterrows(); row.hasNext();){
            if (count == 0) {
                break;
            }
            List<Double> fields = (List<Double>) row.next();
            rows.add(fields.stream().mapToDouble(Double::doubleValue).toArray());
            count--;
        }
        double[][] values = new double[rows.size()][];
        for(int i=0; i< rows.size(); i++){
            values[i] = rows.get(i);
        }
        RealMatrix matrix = MatrixUtils.createRealMatrix(values);
        System.out.println(matrix);
        cointegration_johansen(matrix);
    }
}
