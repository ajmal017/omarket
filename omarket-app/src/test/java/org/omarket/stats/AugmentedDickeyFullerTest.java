package org.omarket.stats;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by christophe on 26/12/16.
 */
public class AugmentedDickeyFullerTest {

    @Test
    public void testLinearTrend() {
        Random rand = new Random();
        double[] x = new double[100];
        for (int i = 0; i < x.length; i++) {
            x[i] = (i + 1) + 5 * rand.nextDouble();
        }
        AugmentedDickeyFuller adf = new AugmentedDickeyFuller(x);
        assertTrue(adf.isNeedsDiff());
    }

    @Test
    public void testLinearTrendWithOutlier() {
        Random rand = new Random();
        double[] x = new double[100];
        for (int i = 0; i < x.length; i++) {
            x[i] = (i + 1) + 5 * rand.nextDouble();
        }
        x[50] = 100;
        AugmentedDickeyFuller adf = new AugmentedDickeyFuller(x);
        assertTrue(adf.isNeedsDiff());
    }

}