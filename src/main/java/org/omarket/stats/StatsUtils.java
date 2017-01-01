package org.omarket.stats;

import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.StatUtils;

import java.util.Arrays;

import static java.lang.Math.min;
import static java.util.Arrays.copyOfRange;

interface MatrixElementOperator{
    public double compute(int row, int column);
}

/**
 * Created by christophe on 30/12/16.
 */
public class StatsUtils {

    public static RealMatrix zeros(int rowDimension, int columnDimension) {
        return MatrixUtils.createRealMatrix(rowDimension, columnDimension);
    }

    public static RealMatrix zeros(int dimension) {
        return zeros(dimension, dimension);
    }

    public static RealMatrix ones(int rowDimension, int columnDimension) {
        return zeros(rowDimension, columnDimension).scalarAdd(1.);
    }

    public static RealMatrix ones(int dimension) {
        return ones(dimension, dimension);
    }

    public static RealMatrix fillValues(int rowDimension, int columnDimension, MatrixElementOperator operator) {
        RealMatrix matrix = MatrixUtils.createRealMatrix(rowDimension, columnDimension);
        for(int row=0; row < rowDimension; row ++){
            for (int column = 0; column < columnDimension; column++){
                matrix.setEntry(row, column, operator.compute(row, column));
            }
        }
        return matrix;
    }

    public static RealMatrix onesLike(RealMatrix other) {
        return ones(other.getRowDimension(), other.getColumnDimension());
    }

    public static double[] columnMeans(RealMatrix matrix){
        RealMatrix ones = StatsUtils.ones(1, matrix.getRowDimension());
        RealMatrix sums = ones.multiply(matrix);
        return sums.scalarMultiply(1. / matrix.getRowDimension()).getRow(0);
    }

    public static double[] rowMeans(RealMatrix matrix){
        RealMatrix ones = StatsUtils.ones(matrix.getColumnDimension(), 1);
        RealMatrix sums = matrix.multiply(ones);
        return sums.scalarMultiply(1. / matrix.getColumnDimension()).getColumn(0);
    }

    public static RealMatrix constantDetrendColumns(RealMatrix matrix){
        RealMatrix output = matrix.copy();
        double[] means = new double[matrix.getColumnDimension()];
        for(int column=0; column < output.getColumnDimension(); column++){
            double[] values = output.getColumn(column);
            means[column] = StatUtils.mean(values);
        }
        for(int row=0; row < output.getRowDimension(); row++){
            for(int column=0; column < output.getColumnDimension(); column++){
                output.setEntry(row, column, output.getEntry(row, column) - means[column]);
            }
        }
        return output;
    }

    public static RealMatrix constantDetrendRows(RealMatrix matrix){
        RealMatrix ones = StatsUtils.ones(matrix.getColumnDimension());
        return matrix.subtract(matrix.multiply(ones).scalarMultiply(1. / matrix.getColumnDimension()));
    }

    public static RealMatrix diffRows(RealMatrix matrix){
        RealMatrix nullifyFirstRow = MatrixUtils.createRealIdentityMatrix(matrix.getRowDimension());
        nullifyFirstRow.setRowVector(0, nullVector(matrix.getRowDimension()));
        return nullifyFirstRow.multiply(matrix.subtract(shiftDown(matrix)));
    }

    public static RealMatrix truncateTop(RealMatrix matrix, int count){
        double[][] data = matrix.getData();
        return MatrixUtils.createRealMatrix(Arrays.copyOfRange(data, count, data.length));
    }

    public static RealMatrix truncateTop(RealMatrix matrix){
        return StatsUtils.truncateTop(matrix, 1);
    }

    public static RealVector nullVector(int dimension){
        return MatrixUtils.createRealVector(zeros(dimension, 1).getColumn(0));
    }

    public static RealMatrix identity(int rowDimension, int columnDimension){
        RealMatrix id = zeros(rowDimension, columnDimension);
        for(int count=0; count < min(rowDimension, columnDimension); count++){
            id.setEntry(count, count, 1.);
        }
        return id;
    }

    public static RealMatrix identityLike(RealMatrix other){
        return identity(other.getRowDimension(), other.getColumnDimension());
    }

    public static RealMatrix shiftDown(RealMatrix matrix, int lag) {
        double[][] data = matrix.getData();
        double[][] shifted = Arrays.copyOfRange(data, 0, data.length - lag);
        double[][] output = new double[data.length][];
        for(int row=lag; row < output.length; row++){
            output[row] = shifted[row - lag];
        }
        for(int row=0; row < lag; row++) {
            output[row] = new double[matrix.getColumnDimension()];
        }
        return MatrixUtils.createRealMatrix(output);
    }

    public static RealMatrix shiftDown(RealMatrix matrix) {
        return shiftDown(matrix, 1);
    }

    public static RealMatrix inverse(RealMatrix matrix) {
        LUDecomposition decomposition = new LUDecomposition(matrix);
        DecompositionSolver solver = decomposition.getSolver();
        return solver.getInverse();
    }
}
