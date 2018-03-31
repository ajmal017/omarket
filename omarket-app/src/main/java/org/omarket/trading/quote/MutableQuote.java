package org.omarket.trading.quote;

/**
 * Created by Christophe on 07/12/2016.
 */
public interface MutableQuote extends Quote {
    boolean updateBestBidSize(int size);

    boolean updateBestAskSize(int size);

    boolean updateBestBidPrice(double price);

    boolean updateBestAskPrice(double price);
}
