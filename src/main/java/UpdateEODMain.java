import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVWriter;
import org.omarket.trading.ContractDB;
import org.omarket.trading.Security;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);
    private final static SimpleDateFormat FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        final String resourceName = "update-eod.json";
        URL resource = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        if (resource == null) {
            throw new RuntimeException("unable to load resource file in classpath: " + resourceName);
        }
        URI resourceURI = resource.toURI();
        Path etfsPath = Paths.get(resourceURI);
        JsonReader reader = new JsonReader(Files.newBufferedReader(etfsPath));
        Type jsonPropertyType = new TypeToken<Map>() {
        }.getType();
        Gson gson = new Gson();
        Map properties = gson.fromJson(reader, jsonPropertyType);
        final ArrayList<String> dbEODPathElements = (ArrayList<String>) properties.get("db.eod.path");
        String dbPath = dbEODPathElements.stream().collect(Collectors.joining(File.separator));
        final ArrayList<String> dbContractsPathElements = (ArrayList<String>) properties.get("db.contracts.path");
        String dbContractsPath = dbContractsPathElements.stream().collect(Collectors.joining(File.separator));
        final boolean onlyCurrentYear = (Boolean)properties.get("current.year.flag");
        final Path eodStorage = Paths.get(dbPath);
        ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
            @Override
            public boolean accept(String content) {
                boolean exchangeMatch = getPrimaryExchange().equals("ARCA");
                boolean typeMatch = getSecurityType().equals("STK");
                boolean currencyMatch = getCurrency().equals("USD");
                return exchangeMatch && typeMatch && currencyMatch;
            }
        };
        Observable<Security> contracts = ContractDB.loadContracts(Paths.get(dbContractsPath), filter);
        contracts
                .map(Security::getSymbol)
                .buffer(20)
                .subscribe(symbols -> {
                    try {
                        logger.info("processing: " + symbols);
                        Map<String, Stock> stocks = YahooFinance.get(symbols.toArray(new String[]{}), true);
                        for (String symbol : stocks.keySet()) {
                            logger.info("processing: " + symbol);
                            Stock stock = stocks.get(symbol);
                            Calendar fromDate = Calendar.getInstance();
                            if (!onlyCurrentYear) {
                                fromDate.add(Calendar.YEAR, -5);
                            }
                            fromDate.set(Calendar.DAY_OF_MONTH, 1);
                            fromDate.set(Calendar.MONTH, Calendar.JANUARY);
                            downloadEOD(stock, eodStorage, fromDate);
                        }
                    } catch (IOException e) {
                        logger.error("failed to retrieve yahoo data", e);
                    }
                }, onError -> {

                });
    }

    private static void downloadEOD(Stock stock, Path eodStorage, Calendar fromDate) throws IOException {
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
        Set<Integer> years = byYear.keySet();
        String exchange = stock.getStockExchange();
        Path exchangeStorage = eodStorage.resolve(exchange);
        String symbol = stock.getSymbol();
        Path stockStorage = exchangeStorage.resolve(symbol.substring(0, 1)).resolve(symbol);
        if (!Files.exists(stockStorage)) {
            Files.createDirectories(stockStorage);
        }
        Path description = stockStorage.resolve("name.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(description)) {
            writer.write(stock.getName());
        }
        for (Integer year : years) {
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

}
