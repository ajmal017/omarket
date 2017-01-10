package org.omarket.trading;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);
    private final static SimpleDateFormat FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");
    ;

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
                .buffer(20)
                .subscribe(symbols -> {
                    try {
                        logger.info("processing: " + symbols);
                        Map<String, Stock> stocks = YahooFinance.get(symbols.toArray(new String[]{}), true);
                        Path eodStorage = Paths.get("data", "eod");
                        for (String symbol : stocks.keySet()) {
                            logger.info("processing: " + symbol);
                            Stock stock = stocks.get(symbol);
                            String exchange = stock.getStockExchange();
                            Path exchangeStorage = eodStorage.resolve(exchange);
                            Path stockStorage = exchangeStorage.resolve(symbol.substring(0, 1)).resolve(symbol);
                            Calendar fromDate = Calendar.getInstance();
                            fromDate.add(Calendar.YEAR, -5);
                            fromDate.set(Calendar.DAY_OF_MONTH, 1);
                            fromDate.set(Calendar.MONTH, Calendar.JANUARY);
                            List<HistoricalQuote> bars = stock.getHistory(fromDate, Interval.DAILY);
                            Map<Integer, Set<HistoricalQuote>> byYear = new TreeMap<>();
                            for (HistoricalQuote bar : bars) {
                                Calendar date = bar.getDate();
                                if (!byYear.containsKey(date.get(Calendar.YEAR))) {
                                    Comparator<HistoricalQuote> sorter = Comparator.comparing(HistoricalQuote::getDate);
                                    byYear.put(date.get(Calendar.YEAR), new TreeSet<>(sorter));
                                }
                                Set<HistoricalQuote> quotes = byYear.get(date.get(Calendar.YEAR));
                                quotes.add(bar);
                            }
                            if (!Files.exists(stockStorage)) {
                                Files.createDirectories(stockStorage);
                            }
                            for (Integer year : byYear.keySet()) {
                                Set<HistoricalQuote> quotes = byYear.get(year);
                                Path yearEOD = stockStorage.resolve(String.valueOf(year) + ".csv");
                                BufferedWriter file = Files.newBufferedWriter(yearEOD, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                                CSVWriter writer = new CSVWriter(file);
                                quotes.forEach(bar -> {
                                    Calendar date = bar.getDate();
                                    BigDecimal high = bar.getHigh();
                                    BigDecimal open = bar.getOpen();
                                    BigDecimal low = bar.getLow();
                                    BigDecimal close = bar.getClose();
                                    Long volume = bar.getVolume();
                                    BigDecimal adjustedClose = bar.getAdjClose();
                                    String[] row = new String[]{
                                            FORMAT_YYYYMMDD.format(date.getTime()),
                                            open.toPlainString(),
                                            high.toPlainString(),
                                            low.toPlainString(),
                                            close.toPlainString(),
                                            adjustedClose.toPlainString(),
                                            String.valueOf(volume)
                                    };
                                    writer.writeNext(row, false);
                                });
                                file.close();
                            }
                        }
                    } catch (IOException e) {
                        logger.error("failed to retrieve yahoo data", e);
                    }
                });
    }

}
