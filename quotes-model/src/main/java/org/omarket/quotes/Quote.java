package org.omarket.quotes;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalUnit;

/**
 * Model of a quotes (bid/ask with volume and timestamp).
 *
 * Created by Christophe on 07/12/2016.
 */
public interface Quote {

    String getProductCode();

    ZonedDateTime getLastModified();

    BigDecimal getBestBidPrice();

    BigDecimal getBestAskPrice();

    Integer getBestBidSize();

    Integer getBestAskSize();

    boolean isValid();

    boolean sameSampledTime(Quote other, TemporalUnit temporalUnit);

}
