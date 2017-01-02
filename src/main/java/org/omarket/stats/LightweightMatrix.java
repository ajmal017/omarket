package org.omarket.stats;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.AbstractRealMatrix;
import org.apache.commons.math3.linear.MatrixDimensionMismatchException;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * Created by Christophe on 02/01/2017.
 */
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

interface ElementProvider{
    double getElement(int row, int column);
}

class ElementProviderWrapper implements ElementProvider{

    final Map<Pair<Integer, Integer>, Double> overrideMap;
    final ElementProvider underlying;

    public ElementProviderWrapper(ElementProvider underlying){
        overrideMap = new HashMap<>();
        this.underlying = underlying;
    }

    @Override
    public double getElement(int row, int column) {
        Pair<Integer, Integer> pair = new ImmutablePair<Integer, Integer>(row, column);
        double value;
        if(overrideMap.containsKey(pair)){
            value = overrideMap.get(pair);
        } else {
            value = underlying.getElement(row, column);
        }
        return value;
    }

    public void override(int row, int column, double value){
        overrideMap.put(new ImmutablePair<>(row, column), value);
    }
}

/**
 * Sparse matrix implementation based on an open addressed map.
 *
 * <p>
 *  Caveat: This implementation assumes that, for any {@code x},
 *  the equality {@code x * 0d == 0d} holds. But it is is not true for
 *  {@code NaN}. Moreover, zero entries will lose their sign.
 *  Some operations (that involve {@code NaN} and/or infinities) may
 *  thus give incorrect results.
 * </p>
 * @since 2.0
 */
public class LightweightMatrix  extends AbstractRealMatrix implements Serializable {
    /** Serializable version identifier. */
    private static final long serialVersionUID = -6462461915057140037L;
    /** Number of rows of the matrix. */
    private final int rows;
    /** Number of columns of the matrix. */
    private final int columns;
    /** Storage for (sparse) matrix elements. */
    private ElementProviderWrapper entries;

    public final static ElementProvider PROVIDER_ZERO = new ElementProvider() {
        @Override
        public double getElement(int row, int column) {
            return 0;
        }
    };

    /**
     * Build a sparse matrix with the supplied row and column dimensions.
     *
     * @param rowDimension Number of rows of the matrix.
     * @param columnDimension Number of columns of the matrix.
     * @throws NotStrictlyPositiveException if row or column dimension is not
     * positive.
     * @throws NumberIsTooLargeException if the total number of entries of the
     * matrix is larger than {@code Integer.MAX_VALUE}.
     */
    public LightweightMatrix(int rowDimension, int columnDimension, ElementProvider provider)
            throws NotStrictlyPositiveException, NumberIsTooLargeException {
        super(rowDimension, columnDimension);
        this.rows = rowDimension;
        this.columns = columnDimension;
        this.entries = new ElementProviderWrapper(provider);
    }

    /**
     * Build a matrix by copying another one.
     *
     * @param matrix matrix to copy.
     */
    public LightweightMatrix(LightweightMatrix matrix) {
        this.rows = matrix.rows;
        this.columns = matrix.columns;
        this.entries = matrix.entries;
    }

    /** {@inheritDoc} */
    @Override
    public LightweightMatrix copy() {
        return new LightweightMatrix(this);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NumberIsTooLargeException if the total number of entries of the
     * matrix is larger than {@code Integer.MAX_VALUE}.
     */
    @Override
    public LightweightMatrix createMatrix(int rowDimension, int columnDimension)
            throws NotStrictlyPositiveException, NumberIsTooLargeException {
        return new LightweightMatrix(rowDimension, columnDimension, PROVIDER_ZERO);
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnDimension() {
        return columns;
    }

    /**
     * Compute the sum of this matrix and {@code m}.
     *
     * @param m Matrix to be added.
     * @return {@code this} + {@code m}.
     * @throws MatrixDimensionMismatchException if {@code m} is not the same
     * size as {@code this}.
     */
    public LightweightMatrix add(LightweightMatrix m)
            throws MatrixDimensionMismatchException {

        MatrixUtils.checkAdditionCompatible(this, m);
        ElementProvider adder = (row, column) -> getEntry(row, column) + m.getEntry(row, column);
        return new LightweightMatrix(this.getRowDimension(), this.getColumnDimension(), adder);
    }

    /** {@inheritDoc} */
    @Override
    public LightweightMatrix subtract(final RealMatrix m)
            throws MatrixDimensionMismatchException {
        try {
            return subtract((LightweightMatrix) m);
        } catch (ClassCastException cce) {
            return (LightweightMatrix) super.subtract(m);
        }
    }

    /**
     * Subtract {@code m} from this matrix.
     *
     * @param m Matrix to be subtracted.
     * @return {@code this} - {@code m}.
     * @throws MatrixDimensionMismatchException if {@code m} is not the same
     * size as {@code this}.
     */
    public LightweightMatrix subtract(LightweightMatrix m)
            throws MatrixDimensionMismatchException {
        MatrixUtils.checkAdditionCompatible(this, m);
        ElementProvider subtracter = (row, column) -> getEntry(row, column) - m.getEntry(row, column);
        return new LightweightMatrix(this.getRowDimension(), this.getColumnDimension(), subtracter);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NumberIsTooLargeException if {@code m} is an
     * {@code OpenMapRealMatrix}, and the total number of entries of the product
     * is larger than {@code Integer.MAX_VALUE}.
     */
    @Override
    public RealMatrix multiply(final RealMatrix m)
            throws DimensionMismatchException, NumberIsTooLargeException {
        try {
            return multiply((LightweightMatrix) m);
        } catch (ClassCastException cce) {

            MatrixUtils.checkMultiplicationCompatible(this, m);

            final int outCols = m.getColumnDimension();
            ElementProvider multiplier = (row, column) -> {
                double value = 0.;
                for(int i=0; i<outCols; i++){
                    value += getEntry(row, i) + m.getEntry(i, column);
                }
                return value;
            };
            return new LightweightMatrix(this.getRowDimension(), this.getColumnDimension(), multiplier);
        }
    }

    /**
     * Postmultiply this matrix by {@code m}.
     *
     * @param m Matrix to postmultiply by.
     * @return {@code this} * {@code m}.
     * @throws DimensionMismatchException if the number of rows of {@code m}
     * differ from the number of columns of {@code this} matrix.
     * @throws NumberIsTooLargeException if the total number of entries of the
     * product is larger than {@code Integer.MAX_VALUE}.
     */
    public LightweightMatrix multiply(LightweightMatrix m)
            throws DimensionMismatchException, NumberIsTooLargeException {
        // Safety check.
        MatrixUtils.checkMultiplicationCompatible(this, m);

        final int outCols = m.getColumnDimension();
        ElementProvider multiplier = (row, column) -> {
            double value = 0.;
            for(int i=0; i<outCols; i++){
                value += getEntry(row, i) + m.getEntry(i, column);
            }
            return value;
        };
        return new LightweightMatrix(this.getRowDimension(), this.getColumnDimension(), multiplier);
    }

    /** {@inheritDoc} */
    @Override
    public double getEntry(int row, int column) throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        return entries.getElement(row, column);
    }

    /** {@inheritDoc} */
    @Override
    public int getRowDimension() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override
    public void setEntry(int row, int column, double value)
            throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        entries.override(row, column, value);
    }

    /** {@inheritDoc} */
    @Override
    public void addToEntry(int row, int column, double increment)
            throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        entries.override(row, column, getEntry(row, column) + increment);
    }

    /** {@inheritDoc} */
    @Override
    public void multiplyEntry(int row, int column, double factor)
            throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        entries.override(row, column, getEntry(row, column) * factor);
    }

}
