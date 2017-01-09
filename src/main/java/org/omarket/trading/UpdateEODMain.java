package org.omarket.trading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);
    private final static SimpleDateFormat FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");;

    public static void main(String[] args) throws InterruptedException, IOException {
        ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
            @Override
            public boolean accept(String content) {
                boolean exchangeMatch = getPrimaryExchange().equals("ARCA");
                boolean typeMatch = getSecurityType().equals("STK");
                boolean currencyMatch = getCurrency().equals("USD");
                return exchangeMatch && typeMatch && currencyMatch;
            }
        };
        Observable<Security> contracts = ContractDB.loadContracts(Paths.get("data", "contracts"), filter);
        contracts
                .map(Security::getSymbol)
                .buffer(10)
                .first()
                .subscribe(symbols -> {
                    try {
                        logger.info("processing: " + symbols);
                        Map<String, Stock> stocks = YahooFinance.get(symbols.toArray(new String[]{}), true);
                        logger.info("retrieved: " + stocks);
                        for(String symbol: stocks.keySet()){
                            logger.info("processing: " + symbol);
                            Stock stock = stocks.get(symbol);
                            Calendar fromDate = Calendar.getInstance();
                            fromDate.add(Calendar.YEAR, -5);
                            fromDate.set(Calendar.DAY_OF_MONTH, 1);
                            fromDate.set(Calendar.MONTH, Calendar.JANUARY);
                            List<HistoricalQuote> bars = stock.getHistory(fromDate, Interval.DAILY);
                            Paths.get("eod", "contracts");
                            String exchange = stock.getStockExchange();
                            for(HistoricalQuote bar: bars){
                                Calendar date = bar.getDate();
                                BigDecimal high = bar.getHigh();
                                BigDecimal open = bar.getOpen();
                                BigDecimal low = bar.getLow();
                                BigDecimal close = bar.getClose();
                                Long volume = bar.getVolume();
                                BigDecimal adjustedClose = bar.getAdjClose();
                                logger.info("date: " + FORMAT_YYYYMMDD.format(date));
                                logger.info("close: " + adjustedClose);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("failed to retrieve yahoo data", e);
                    }
                });
    }

}
