package org.omarket.trading;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

/**
 * Created by christophe on 24.09.16.
 */
public class ProfitAndLossTrackerTest {
    private static Logger logger = LoggerFactory.getLogger(ProfitAndLossTrackerTest.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicUsage() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5));
        assertEquals(100, pos.getQuantity().intValue());
        assertEquals(500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);

        pos.addFill(-100, BigDecimal.valueOf(5));
        assertEquals(0, pos.getQuantity().intValue());
        assertEquals(0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void basicInverted() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(-100, BigDecimal.valueOf(5));
        assertEquals(-100, pos.getQuantity().intValue());
        assertEquals(-500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);

        pos.addFill(100, BigDecimal.valueOf(5));
        assertEquals(0, pos.getQuantity().intValue());
        assertEquals(0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void profit() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5));
        assertEquals(100, pos.getQuantity().intValue());
        assertEquals(500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);
        pos.addFill(-100, BigDecimal.valueOf(6));
        assertEquals(0, pos.getQuantity().intValue());
        assertEquals(0, pos.getAcquisitionCost(), 1E6);
        assertEquals(100, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void profitInverted() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(-100, BigDecimal.valueOf(5));
        assertEquals(-100, pos.getQuantity().intValue());
        assertEquals(-500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0, pos.getRealizedPnl(), 1E6);
        pos.addFill(100, BigDecimal.valueOf(4));
        assertEquals(0, pos.getQuantity().intValue());
        assertEquals(0, pos.getAcquisitionCost(), 1E6);
        assertEquals(100, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void realDataJNK() throws Exception {

        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(203, BigDecimal.valueOf(38.7556));
        assertEquals(pos.getRealizedPnl(), 0., 1E6);
        assertEquals(pos.getUnrealizedPnl(BigDecimal.valueOf(38.7556)), 0., 1E6);
        pos.addFill(-203, BigDecimal.valueOf(38.7654));
        assertEquals(pos.getRealizedPnl(), 1.989400, 1E6);
        assertEquals(pos.getUnrealizedPnl(BigDecimal.valueOf(38.7654)), 0., 1E6);
        pos.addFill(-203, BigDecimal.valueOf(38.7950));
        assertEquals(pos.getRealizedPnl(), 0., 1E6);
        assertEquals(pos.getUnrealizedPnl(BigDecimal.valueOf(38.7950)), 0., 1E6);
        pos.addFill(203, BigDecimal.valueOf(38.8443));
        assertEquals(pos.getRealizedPnl(), 203. * (38.7950 - 38.8443), 1E6);
        assertEquals(pos.getUnrealizedPnl(BigDecimal.valueOf(38.8443)), 0., 1E6);
    }

    @Test
    public void stack() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(1, BigDecimal.valueOf(80.0));
        pos.addFill(-3, BigDecimal.valueOf(102.0));
        pos.addFill(-2, BigDecimal.valueOf(98.0));
        pos.addFill(3, BigDecimal.valueOf(90.0));
        pos.addFill(-2, BigDecimal.valueOf(100.0));
        assertEquals(-3, pos.getQuantity().longValue());
        assertEquals(-300., pos.getAcquisitionCost(), 1E6);
        assertEquals(52., pos.getRealizedPnl(), 1E6);
        assertEquals(-3., pos.getUnrealizedPnl(BigDecimal.valueOf(101.)), 1E6);
        assertEquals(49., pos.getTotalPnl(BigDecimal.valueOf(101.)), 1E6);
    }

    @Test
    public void loss() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5.0));
        assertEquals(100, pos.getQuantity().longValue());
        assertEquals(500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(-100, BigDecimal.valueOf(4.0));
        assertEquals(0, pos.getQuantity().longValue());
        assertEquals(0.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(-100.0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void lossInverted() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(-100, BigDecimal.valueOf(5.0));
        assertEquals(-100, pos.getQuantity().longValue());
        assertEquals(-500.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(100, BigDecimal.valueOf(6.0));
        assertEquals(0, pos.getQuantity().longValue());
        assertEquals(0.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(-100.0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void partialFill() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5.0));
        pos.addFill(-25, BigDecimal.valueOf(5.0));
        assertEquals(75, pos.getQuantity().longValue());
        assertEquals(375.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(-50, BigDecimal.valueOf(6.0));
        assertEquals(25, pos.getQuantity().longValue());
        assertEquals(125.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(50.0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void addFill() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5.0));
        pos.addFill(25, BigDecimal.valueOf(4.0));
        assertEquals(125, pos.getQuantity().longValue());
        assertEquals(600.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(-50, BigDecimal.valueOf(6.0));
        assertEquals(75, pos.getQuantity().longValue());
        assertEquals(360.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(60.0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void flip() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(100, BigDecimal.valueOf(5.0));
        pos.addFill(25, BigDecimal.valueOf(4.0));
        assertEquals(125, pos.getQuantity().longValue());
        assertEquals(600.0, pos.getAcquisitionCost(), 1E6);
        assertEquals(0.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(-150, BigDecimal.valueOf(6.0));
        assertEquals(-25, pos.getQuantity().longValue());
        assertEquals(-150., pos.getAcquisitionCost(), 1E6);
        assertEquals(150.0, pos.getRealizedPnl(), 1E6);
    }

    @Test
    public void flipInverted() throws Exception {
        ProfitAndLossTracker pos = new ProfitAndLossTracker();
        pos.addFill(-100, BigDecimal.valueOf(5.0));
        pos.addFill(-25, BigDecimal.valueOf(5.5));
        pos.addFill(50, BigDecimal.valueOf(4.));
        assertEquals(-75, pos.getQuantity().longValue());
        assertEquals(-382.5, pos.getAcquisitionCost(), 1E6);
        assertEquals(55.0, pos.getRealizedPnl(), 1E6);

        pos.addFill(100, BigDecimal.valueOf(4.75));
        assertEquals(25, pos.getQuantity().longValue());
        assertEquals(118.75, pos.getAcquisitionCost(), 1E6);
        assertEquals(81.25, pos.getRealizedPnl(), 1E6);

        pos.addFill(-25, BigDecimal.valueOf(4.50));
        assertEquals(0, pos.getQuantity().longValue());
        assertEquals(0., pos.getAcquisitionCost(), 1E6);
        assertEquals(75., pos.getRealizedPnl(), 1E6);
    }


}
