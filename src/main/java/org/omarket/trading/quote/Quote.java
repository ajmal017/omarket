package org.omarket.trading.quote;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by Christophe on 07/12/2016.
 */
public interface Quote {
    Date getLastModified();

    BigDecimal getBestBidPrice();

    BigDecimal getBestAskPrice();

    Integer getBestBidSize();

    Integer getBestAskSize();

    boolean isValid();

    boolean sameSampledTime(Quote other, Quote.Sampling frequency);

    enum Sampling {
        SECOND, MINUTE, HOUR
    }
}
