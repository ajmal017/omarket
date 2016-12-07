package org.omarket.trading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 * Created by christophe on 24.09.16.
 * <p>
 * Computes P&L based on weighted average acquisitionCost method.
 */
public class ProfitAndLossTracker {
    private static Logger logger = LoggerFactory.getLogger(ProfitAndLossTracker.class);
    private Integer quantity = 0;
    private float acquisitionCost = 0;
    private float realizedPnl = 0;

    public float getUnitAcquisitionCost() {
        return this.acquisitionCost / this.quantity;
    }

    public float getMarketValue(BigDecimal currentPrice) {
        return currentPrice.multiply(BigDecimal.valueOf(this.quantity)).floatValue();
    }

    public float getUnrealizedPnl(BigDecimal currentPrice) {
        return this.getMarketValue(currentPrice) - this.acquisitionCost;
    }

    public float getTotalPnl(BigDecimal currentPrice) {
        return this.realizedPnl + this.getUnrealizedPnl(currentPrice);
    }


    public void addFill(Integer fillQty, BigDecimal fillPrice) {
        /* Adding a fill updates the P&L intermediary data.
         */
        logger.debug("adding fill: {} at {}", fillQty, fillPrice);
        Integer oldQuantity = this.quantity;
        float oldCost = this.acquisitionCost;
        float oldRealized = this.realizedPnl;

        if (oldQuantity == 0) {
            this.quantity = fillQty;
            this.acquisitionCost = fillPrice.multiply(BigDecimal.valueOf(fillQty)).floatValue();
            this.realizedPnl = 0;
        } else {
            Integer closingQty = 0;
            Integer openingQty = fillQty;
            if (Math.signum(oldQuantity) != Math.signum(fillQty)) {
                closingQty = min(abs(oldQuantity), abs(fillQty)) * (int) Math.signum(fillQty);
                openingQty = fillQty - closingQty;
            }

            this.quantity = oldQuantity + fillQty;
            this.acquisitionCost = oldCost + openingQty * fillPrice.floatValue() + closingQty.floatValue() * oldCost / oldQuantity;
            this.realizedPnl = oldRealized + closingQty.floatValue() * (oldCost / oldQuantity - fillPrice.floatValue());
        }
    }

    public Integer getQuantity() {
        return quantity;
    }

    public float getAcquisitionCost() {
        return acquisitionCost;
    }

    public float getRealizedPnl() {
        return realizedPnl;
    }
}


