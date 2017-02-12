import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVWriter;
import org.apache.commons.io.input.ReversedLinesFileReader;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.TemporalAdjusters;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);
    private final static SimpleDateFormat FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        final String resourceName = "update-eod.json";
        YahooFinance.logger.setLevel(java.util.logging.Level.WARNING);
        logger.info("starting EOD update");
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
                .buffer(1)
                .subscribe(symbols -> {
                    try {
                        logger.info("processing: " + symbols);
                        Map<String, Stock> stocks = YahooFinance.get(symbols.toArray(new String[]{}), true);
                        for (String symbol : stocks.keySet()) {
                            Stock stock = stocks.get(symbol);
                            if(isUpdateToDate(eodStorage, stock.getStockExchange(), stock.getSymbol())){
                                logger.debug("already up-to-date: " + symbol);
                                continue;
                            }
                            LocalDate fromDate = LocalDate.now();
                            if (!onlyCurrentYear) {
                                fromDate = fromDate.minus(Period.ofYears(5));
                            }
                            fromDate = fromDate.withDayOfYear(1);
                            logger.info("downloading: " + symbol + " from " + fromDate.format(ISO_LOCAL_DATE));
                            downloadEOD(stock, eodStorage, fromDate);
                        }
                    } catch (IOException e) {
                        logger.error("failed to retrieve yahoo data", e);
                    }
                }, onError -> {
                    logger.error("failed to process contracts", onError);
                });
    }

    private static Path getEODPath(Path eodStorage, String exchange, String symbol){
        Path exchangeStorage = eodStorage.resolve(exchange);
        return exchangeStorage.resolve(symbol.substring(0, 1)).resolve(symbol);
    }

    private static boolean isUpdateToDate(Path eodStorage, String exchange, String symbol) {
        Path eodPath = getEODPath(eodStorage, exchange, symbol);
        LocalDate today = LocalDate.now();
        LocalDate lastBusinessDay;
        if(today.getDayOfWeek() == DayOfWeek.SATURDAY
                || today.getDayOfWeek() == DayOfWeek.SUNDAY
                || today.getDayOfWeek() == DayOfWeek.MONDAY){
           lastBusinessDay = today.with(TemporalAdjusters.previous(DayOfWeek.FRIDAY));
        } else {
            lastBusinessDay = today;
        }
        Path currentFile = eodPath.resolve(String.valueOf(lastBusinessDay.getYear()) + ".csv");
        if (!Files.exists(eodPath)) {
            return false;
        }
        try (ReversedLinesFileReader reader = new ReversedLinesFileReader(currentFile.toFile(), StandardCharsets.UTF_8)) {
            String lastLine = reader.readLine();
            String lastYYYYMMDD = lastLine.split(",")[0];
            if(lastBusinessDay.format(BASIC_ISO_DATE).equals(lastYYYYMMDD)){
                return true;
            }
        } catch (IOException e) {
            logger.error("failed to access EOD database", e);
        }
        return false;

    }

    private static void downloadEOD(Stock stock, Path eodStorage, LocalDate fromDate) throws IOException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(java.sql.Date.valueOf(fromDate));
        List<HistoricalQuote> bars = stock.getHistory(calendar, Interval.DAILY);
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
        String symbol = stock.getSymbol();
        Path stockStorage = getEODPath(eodStorage, exchange, symbol);
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
