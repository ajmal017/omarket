package org.omarket.stats;

import org.apache.commons.math3.linear.*;

import java.util.*;

import static java.lang.Math.log;

/**
 * Created by christophe on 30/12/16.
 */
public class CoIntegration {

    private final static RealMatrix _ECJP0 = MatrixUtils.createRealMatrix(new double[][]{
            {2.9762, 4.1296, 6.9406},
            {9.4748, 11.2246, 15.0923},
            {15.7175, 17.7961, 22.2519},
            {21.8370, 24.1592, 29.0609},
            {27.9160, 30.4428, 35.7359},
            {33.9271, 36.6301, 42.2333},
            {39.9085, 42.7679, 48.6606},
            {45.8930, 48.8795, 55.0335},
            {51.8528, 54.9629, 61.3449},
            {57.7954, 61.0404, 67.6415},
            {63.7248, 67.0756, 73.8856},
            {69.6513, 73.0946, 80.0937},
    });

    private final static RealMatrix _ECJP1 = MatrixUtils.createRealMatrix(new double[][]{
            {2.7055, 3.8415, 6.6349},
            {12.2971, 14.2639, 18.5200},
            {18.8928, 21.1314, 25.8650},
            {25.1236, 27.5858, 32.7172},
            {31.2379, 33.8777, 39.3693},
            {37.2786, 40.0763, 45.8662},
            {43.2947, 46.2299, 52.3069},
            {49.2855, 52.3622, 58.6634},
            {55.2412, 58.4332, 64.9960},
            {61.2041, 64.5040, 71.2525},
            {67.1307, 70.5392, 77.4877},
            {73.0563, 76.5734, 83.7105},
    });

    private final static RealMatrix _ECJP2 = MatrixUtils.createRealMatrix(new double[][]{
            {2.7055, 3.8415, 6.6349},
            {15.0006, 17.1481, 21.7465},
            {21.8731, 24.2522, 29.2631},
            {28.2398, 30.8151, 36.1930},
            {34.4202, 37.1646, 42.8612},
            {40.5244, 43.4183, 49.4095},
            {46.5583, 49.5875, 55.8171},
            {52.5858, 55.7302, 62.1741},
            {58.5316, 61.8051, 68.5030},
            {64.5292, 67.9040, 74.7434},
            {70.4630, 73.9355, 81.0678},
            {76.4081, 79.9878, 87.2395},
    });

    private final static RealMatrix _TCJP0 = MatrixUtils.createRealMatrix(new double[][]{
            {2.9762, 4.1296, 6.9406},
            {10.4741, 12.3212, 16.3640},
            {21.7781, 24.2761, 29.5147},
            {37.0339, 40.1749, 46.5716},
            {56.2839, 60.0627, 67.6367},
            {79.5329, 83.9383, 92.7136},
            {106.7351, 111.7797, 121.7375},
            {137.9954, 143.6691, 154.7977},
            {173.2292, 179.5199, 191.8122},
            {212.4721, 219.4051, 232.8291},
            {255.6732, 263.2603, 277.9962},
            {302.9054, 311.1288, 326.9716},
    });

    private final static RealMatrix _TCJP1 = MatrixUtils.createRealMatrix(new double[][]{
            {2.7055, 3.8415, 6.6349},
            {13.4294, 15.4943, 19.9349},
            {27.0669, 29.7961, 35.4628},
            {44.4929, 47.8545, 54.6815},
            {65.8202, 69.8189, 77.8202},
            {91.1090, 95.7542, 104.9637},
            {120.3673, 125.6185, 135.9825},
            {153.6341, 159.5290, 171.0905},
            {190.8714, 197.3772, 210.0366},
            {232.1030, 239.2468, 253.2526},
            {277.3740, 285.1402, 300.2821},
            {326.5354, 334.9795, 351.2150},
    });

    private final static RealMatrix _TCJP2 = MatrixUtils.createRealMatrix(new double[][]{
            {2.7055, 3.8415, 6.6349},
            {16.1619, 18.3985, 23.1485},
            {32.0645, 35.0116, 41.0815},
            {51.6492, 55.2459, 62.5202},
            {75.1027, 79.3422, 87.7748},
            {102.4674, 107.3429, 116.9829},
            {133.7852, 139.2780, 150.0778},
            {169.0618, 175.1584, 187.1891},
            {208.3582, 215.1268, 228.2226},
            {251.6293, 259.0267, 273.3838},
            {298.8836, 306.8988, 322.4264},
            {350.1125, 358.7190, 375.3203},
    });


    /**
     * Critical values for Johansen trace statistic.
     * The order of time polynomial in the null-hypothesis allows following values:
     * - p = -1, no deterministic part
     * - p =  0, for constant term
     * - p =  1, for constant plus time-trend
     * - p >  1  returns no critical values
     * :param dim_index:
     * :param time_polynomial_order: order of time polynomial in the null-hypothesis
     * :return:
     */
    public static RealVector get_critical_values_trace(int dim_index, int time_polynomial_order) {
        RealVector jc = null;
        if (time_polynomial_order < -1 || time_polynomial_order > 1) {
            jc = MatrixUtils.createRealVector(new double[]{0., 0., 0.});
        } else if (dim_index < 1 || dim_index > 12) {
            jc = MatrixUtils.createRealVector(new double[]{0., 0., 0.});
        } else if (time_polynomial_order == -1) {
            jc = _TCJP0.getRowVector(dim_index - 1);
        } else if (time_polynomial_order == 0) {
            jc = _TCJP1.getRowVector(dim_index - 1);
        } else if (time_polynomial_order == 1) {
            jc = _TCJP2.getRowVector(dim_index - 1);
        }
        return jc;
    }

    /**
     * Critical values for Johansen maximum eigenvalue statistic.
     * The order of time polynomial in the null-hypothesis allows following values:
     * - p = -1, no deterministic part
     * - p =  0, for constant term
     * - p =  1, for constant plus time-trend
     * - p >  1  returns no critical values
     * :param dim_index:
     * :param time_polynomial_order: order of time polynomial in the null-hypothesis
     * :return:
     */
    public static RealVector get_critical_values_max_eigenvalue(int dim_index, int time_polynomial_order) {
        RealVector jc = null;
        if (time_polynomial_order < -1 || time_polynomial_order > 1) {
            jc = MatrixUtils.createRealVector(new double[]{0., 0., 0.});
        } else if (dim_index < 1 || dim_index > 12) {
            jc = MatrixUtils.createRealVector(new double[]{0., 0., 0.});
        } else if (time_polynomial_order == -1) {
            jc = _ECJP0.getRowVector(dim_index - 1);
        } else if (time_polynomial_order == 0) {
            jc = _ECJP1.getRowVector(dim_index - 1);
        } else if (time_polynomial_order == 1) {
            jc = _ECJP2.getRowVector(dim_index - 1);
        }
        return jc;
    }

    public static RealVector residuals(RealVector y, RealMatrix x) {
        if (x.getColumnDimension() == 0 || x.getRowDimension() == 0) {
            return y;
        }
        return y.subtract(x.operate(pinv(x).operate(y)));
    }

    public static RealMatrix pinv(RealMatrix a){
        QRDecomposition decomposition = new QRDecomposition(a);
        DecompositionSolver solver = decomposition.getSolver();
        return solver.getInverse();
    }
    public static Result cointegration_johansen(RealMatrix input_df){
        return cointegration_johansen(input_df, 1);

    }
    public static Result cointegration_johansen(RealMatrix input, int lag){
        RealMatrix x = StatsUtils.constantDetrendColumns(input);
        RealMatrix dx = StatsUtils.truncateTop(StatsUtils.diffRows(x));
        RealMatrix z = StatsUtils.constantDetrendColumns(StatsUtils.truncateTop(StatsUtils.shiftDown(dx, lag), lag));
        RealMatrix shiftedDx = StatsUtils.constantDetrendColumns(StatsUtils.truncateTop(dx, lag));
        QRDecomposition qr = new QRDecomposition(z);
        DecompositionSolver solver = qr.getSolver();
        RealMatrix r0t = shiftedDx.subtract(z.multiply(solver.solve(shiftedDx)));
        RealMatrix shiftedDx2 = StatsUtils.constantDetrendColumns((StatsUtils.truncateTop(StatsUtils.shiftDown(x, lag), lag+1)));
        RealMatrix rkt = shiftedDx2.subtract(z.multiply(solver.solve(shiftedDx2)));

        RealMatrix skk = rkt.transpose().multiply(rkt).scalarMultiply(1. / rkt.getRowDimension());
        RealMatrix sk0 = rkt.transpose().multiply(r0t).scalarMultiply(1. / rkt.getRowDimension());
        RealMatrix s00 = r0t.transpose().multiply(r0t).scalarMultiply(1. / r0t.getRowDimension());
        RealMatrix sig = sk0.multiply(StatsUtils.inverse(s00)).multiply(sk0.transpose());
        EigenDecomposition decomposition = new EigenDecomposition(StatsUtils.inverse(skk).multiply(sig));
        double[] eigenValues = decomposition.getRealEigenvalues();
        Map<Double, RealVector> vectors = new TreeMap<>();
        for(int i=0; i<eigenValues.length; i++){
            vectors.put(eigenValues[i], decomposition.getEigenvector(i));
        }
        RealMatrix eigenVectors = decomposition.getV();
        RealMatrix sortedVectors = MatrixUtils.createRealMatrix(eigenVectors.getRowDimension(), eigenVectors.getColumnDimension());
        int count = vectors.size() - 1;
        for(Double eigenvalue: vectors.keySet()){
            RealVector eigenvector = vectors.get(eigenvalue);
            for(int row=0; row < eigenvector.getDimension(); row++){
                sortedVectors.setEntry(row, count, eigenvector.getEntry(row));
            }
            count--;
        }
        // Normalizes the eigen vectors such that (du'skk*du) = I
        RealMatrix eigenvectorsNormalizer = sortedVectors.transpose().multiply(skk).multiply(sortedVectors);

        CholeskyDecomposition cholesky = new CholeskyDecomposition(eigenvectorsNormalizer, 1., 1.E-9D);
        RealMatrix dt = sortedVectors.multiply(StatsUtils.inverse((cholesky.getL())));
        int m = input.getColumnDimension();
        RealVector lr1 = StatsUtils.zerosVector(m);
        RealVector lr2 = StatsUtils.zerosVector(m);
        RealMatrix cvm = StatsUtils.zeros(m,3);
        RealMatrix cvt = StatsUtils.zeros(m,3);
        RealVector iota = StatsUtils.onesVector(m);
        int t = rkt.getRowDimension();
        /* Computes the trace and max eigenvalue statistics */

        List<Double> sortedValues = new LinkedList<>();
        for(Double value: vectors.keySet()){
            sortedValues.add(value);
        }
        Collections.reverse(sortedValues);
        RealVector sortedEigenvalues = StatsUtils.createVector(sortedValues);
        for(int i=0; i < m; i++){
            RealVector lr1Tmp = iota.subtract(sortedEigenvalues).map(Math::log);
            RealVector lr1TmpTruncated = StatsUtils.truncateTop(lr1Tmp, i);
            lr1.setEntry(i, -t * StatsUtils.sum(lr1TmpTruncated));
            double lr2Tmp = -t * log(1. - sortedEigenvalues.getEntry(i));
            lr2.setEntry(i, lr2Tmp);
            cvm.setRowVector(i, get_critical_values_max_eigenvalue(m - i, 0));
            cvt.setRowVector(i, get_critical_values_trace(m - i, 0));
        }
        return new Result(sortedEigenvalues, dt, lr1, lr2, cvt, cvm);
    }

    static class Result{
        private final RealVector eigenValues;
        private final RealMatrix eigenVectors;
        private final RealVector lr1;
        private final RealVector lr2;
        private final RealMatrix cvt;
        private final RealMatrix cvm;

        public Result(RealVector eigenValues, RealMatrix eigenVectors, RealVector lr1, RealVector lr2, RealMatrix cvt, RealMatrix cvm) {
            this.eigenValues = eigenValues;
            this.eigenVectors = eigenVectors;
            this.lr1 = lr1;
            this.lr2 = lr2;
            this.cvt = cvt;
            this.cvm = cvm;
        }

        public RealVector getEigenValues() {
            return eigenValues;
        }

        /***
         * Eigenvectors arranged as column vectors.
         *
         * @return
         */
        public RealMatrix getEigenVectors() {
            return eigenVectors;
        }

        /**
         * Likelihood ratio trace statistic for r=0 to m-1.
         *
         * @return vector
         */
        public RealVector getLikelihoodRatioTraceStatistics() {
            return lr1;
        }

        /**
         * Maximum eigenvalue statistic for r=0 to m-1.
         *
         * @return vector
         */
        public RealVector getEigenvalueStatistics() {
            return lr2;
        }

        /**
         * Critical values for trace statistic.
         *
         * @return 3 row vectors [90% 95% 99%]
         */
        public RealMatrix getCriticalValuesTraceStatistics() {
            return cvt;
        }

        /**
         * Critical values for max eigenvalue statistic.
         *
         * @return 3 row vectors [90% 95% 99%]
         */
        public RealMatrix getCriticalValuesMaxEigenvalue() {
            return cvm;
        }

        public String toString(){
            Map<String, String> entries = new LinkedHashMap<>();
            RealMatrixFormat toOctave = MatrixUtils.OCTAVE_FORMAT;
            entries.put("eigenvalues", eigenValues.toString());
            entries.put("eigenvectors", toOctave.format(eigenVectors));
            entries.put("lr1", lr1.toString());
            entries.put("lr2", lr2.toString());
            entries.put("cvt", toOctave.format(cvt));
            entries.put("cvm", toOctave.format(cvm));

            StringBuilder sb = new StringBuilder();
            Iterator<String> iter = entries.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                String entry = entries.get(key);
                sb.append(key);
                sb.append('=').append('"');
                sb.append(entry);
                sb.append('"');
                if (iter.hasNext()) {
                    sb.append(',').append("%n");
                }
            }
            return String.format(sb.toString());
        }
    }
}
