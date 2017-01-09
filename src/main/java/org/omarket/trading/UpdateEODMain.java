package org.omarket.trading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);

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
                .map(details -> {
                    return details.getSymbol();
                })
                .buffer(10)
                .first()
                .subscribe(symbols -> {
                    try {
                        logger.info("processing: " + symbols);
                        Map<String, Stock> stocks = YahooFinance.get(symbols.toArray(new String[]{}), true);
                        logger.info("retrieved: " + stocks);
                    } catch (IOException e) {
                        logger.error("failed to retrieve yahoo data", e);
                    }
                });
    }

}
