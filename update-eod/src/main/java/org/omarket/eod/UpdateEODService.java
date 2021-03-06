package org.omarket.eod;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.ContractFilter;
import org.omarket.contractinfo.Security;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.Observable;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

@Slf4j
@Component
public class UpdateEODService {

    private final static SimpleDateFormat FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");
    private final ContractDBService contractDBservice;
    private final Path dbContractsPath;
    private final boolean onlyCurrentYear;
    private Path storageEOD;

    @Autowired
    public UpdateEODService(ContractDBService contractDBservice) {
        final String resourceName = "update-eod.json";
        log.info("starting EOD update");
        InputStream resourceStream = ClassLoader.getSystemResourceAsStream(resourceName);
        JsonReader jsonReader = new JsonReader(new InputStreamReader(resourceStream));
        JsonParser parser = new JsonParser();
        JsonObject properties = parser.parse(jsonReader).getAsJsonObject();
        String dbPath = propertiesArrayJoin(properties.get("db.eod.path"), File.separator);
        dbContractsPath = Paths.get(propertiesArrayJoin(properties.get("db.contracts.path"), File.separator));
        onlyCurrentYear = properties.get("current.year.flag").getAsBoolean();
        final Path eodStorage = Paths.get(dbPath);
        setStorageEOD(eodStorage);
        this.contractDBservice = contractDBservice;
    }

    private static String propertiesArrayJoin(JsonElement input, String separator) {
        String[] elements = new Gson().fromJson(input.getAsJsonArray(), String[].class);
        return Arrays.stream(elements).collect(Collectors.joining(separator));
    }

    public void update() throws IOException {
        ContractFilter filter = new ContractFilter() {
            @Override
            public boolean accept(String content) {
                boolean exchangeMatch = getPrimaryExchange().equals("ARCA");
                boolean typeMatch = getSecurityType().equals("STK");
                boolean currencyMatch = getCurrency().equals("USD");
                return exchangeMatch && typeMatch && currencyMatch;
            }
        };
        Observable<Security> contracts = contractDBservice.loadContracts(getDbContractsPath(), filter);
        contracts
                .subscribe(contract -> {
                    try {
                        String symbol = contract.getSymbol();
                        log.debug("processing: " + symbol);
                        String yahooExchange = findExchange(symbol);
                        if (yahooExchange == null || yahooExchange.equals("OLD")) {
                            log.info("ignoring old security: " + symbol);
                        } else if (isUpdateToDate(yahooExchange, symbol)) {
                            log.debug("already up-to-date: " + symbol);
                        } else {
                            LocalDate fromDate = LocalDate.now();
                            if (!isOnlyCurrentYear()) {
                                fromDate = fromDate.minus(Period.ofYears(2));
                            }
                            fromDate = fromDate.withDayOfYear(1);
                            log.info("downloading: " + symbol + " from " + fromDate.format(ISO_LOCAL_DATE));
                            downloadEOD(symbol, fromDate);
                        }
                    } catch (IOException e) {
                        log.error("failed to retrieve yahoo data", e);
                        System.exit(-1);
                    } catch (YahooAccessException e) {
                        log.error("critical error while accessing Yahoo data", e);
                    }
                }, onError -> {
                    log.error("failed to process contracts", onError);
                    System.exit(-1);
                });
    }

    private boolean isUpdateToDate(String exchange, String symbol) {
        Path eodPath = getEODPath(exchange, symbol);
        LocalDate day = LocalDate.now().minusDays(1);
        LocalDate lastBusinessDay;
        if (day.getDayOfWeek() == DayOfWeek.SATURDAY
                || day.getDayOfWeek() == DayOfWeek.SUNDAY) {
            lastBusinessDay = day.with(TemporalAdjusters.previous(DayOfWeek.FRIDAY));
        } else {
            lastBusinessDay = day;
        }
        Path currentFile = eodPath.resolve(String.valueOf(lastBusinessDay.getYear()) + ".csv");
        if (!Files.exists(eodPath)) {
            return false;
        }
        if (!Files.exists(currentFile, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Files.createFile(currentFile);
            } catch (IOException e) {
                log.error(format("unable to create file %s", currentFile), e);
            }
        }
        try (ReversedLinesFileReader reader = new ReversedLinesFileReader(currentFile.toFile(), StandardCharsets.UTF_8)) {
            String lastLine = reader.readLine();
            if (lastLine == null) {
                return false;
            } else {
                String lastYYYYMMDD = lastLine.split(",")[0];
                if (lastBusinessDay.format(BASIC_ISO_DATE).equals(lastYYYYMMDD)) {
                    return true;
                }
            }
        } catch (IOException e) {
            log.error("failed to access EOD database", e);
        }
        return false;

    }

    private String findExchange(String symbol) {
        Path cacheStorage = getStorageEOD().resolve("cache.json");
        Gson gson = new Gson();
        Map<String, Map<String, String>> cache;
        String exchange;
        if (Files.exists(cacheStorage)) {
            Type CACHE_TYPE = new TypeToken<Map<String, Map<String, String>>>() {
            }.getType();
            BufferedReader bufferedReader;
            try {
                bufferedReader = Files.newBufferedReader(cacheStorage);
                try (JsonReader reader = new JsonReader(bufferedReader)) {
                    cache = gson.fromJson(reader, CACHE_TYPE);
                    if (cache.containsKey(symbol)) {
                        return cache.get(symbol).get("stockExchange");
                    }
                } catch (IOException e) {
                    log.error("failed to access cache storage", e);
                    return null;
                }
            } catch (IOException e) {
                log.error("failed to access cache storage", e);
                return null;
            }
        } else {
            try {
                Files.createFile(cacheStorage);
            } catch (IOException e) {
                log.error("failed to access cache storage", e);
                return null;
            }
            cache = new HashMap<>();
        }
        Stock stock;
        try {
            stock = YahooFinance.get(symbol, false);
        } catch (IOException e) {
            log.error("failed to access Yahoo Finance", e);
            return null;
        }
        Map<String, String> stockData = new HashMap<>();
        stockData.put("stockExchange", stock.getStockExchange());
        stockData.put("currency", stock.getCurrency());
        stockData.put("name", stock.getName());
        cache.put(symbol, stockData);
        try (BufferedWriter writer = Files.newBufferedWriter(cacheStorage, StandardCharsets.UTF_8)) {
            String json = gson.toJson(cache);
            writer.write(json);
        } catch (IOException e) {
            log.error("failed to access cache storage", e);
        }
        exchange = stock.getStockExchange();
        return exchange;
    }

    private Path getEODPath(String exchange, String symbol) {
        Path exchangeStorage = getStorageEOD().resolve(exchange);
        return exchangeStorage.resolve(symbol.substring(0, 1)).resolve(symbol);
    }

    private void downloadEOD(String symbol, LocalDate fromDate) throws IOException, YahooAccessException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(java.sql.Date.valueOf(fromDate));
        try {
            Stock stock = YahooFinance.get(symbol, true);
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
            Path stockStorage = getEODPath(exchange, symbol);
            if (!Files.exists(stockStorage)) {
                Files.createDirectories(stockStorage);
            }
            Path description = stockStorage.resolve("name.txt");
            try (BufferedWriter writer = Files.newBufferedWriter(description)) {
                writer.write(stock.getName());
            }
            final String[] lastBarDate = {null};
            for (Integer year : years) {
                Set<HistoricalQuote> quotes = byYear.get(year);
                if (quotes == null) {
                    log.warn(format("year %s not forund for stock %s", year, stock));
                    continue;
                }
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
                    if ((date != null) && (high != null) && (open != null) && (low != null) && (close != null)) {
                        String barDate = FORMAT_YYYYMMDD.format(date.getTime());
                        String[] row = new String[]{
                                FORMAT_YYYYMMDD.format(date.getTime()),
                                open.toPlainString(),
                                high.toPlainString(),
                                low.toPlainString(),
                                close.toPlainString(),
                                adjustedClose.toPlainString(),
                                String.valueOf(volume)
                        };
                        lastBarDate[0] = barDate;
                        writer.writeNext(row, false);
                    } else {
                        log.warn(format("ignoring invalid quotes: %s", bar));
                    }
                });
                file.close();
            }
            if (lastBarDate[0] != null) {
                log.info("saved data for stock " + stock.getSymbol() + " up to: " + lastBarDate[0]);

            } else {
                log.info("no data saved for stock " + stock.getSymbol());
            }

        } catch (java.io.FileNotFoundException fileNotFoundException) {
            throw new YahooAccessException(fileNotFoundException);
        }
    }

    public Path getStorageEOD() {
        return storageEOD;
    }

    private void setStorageEOD(Path storageEOD) {
        this.storageEOD = storageEOD;
    }

    public Path getDbContractsPath() {
        return dbContractsPath;
    }

    public boolean isOnlyCurrentYear() {
        return onlyCurrentYear;
    }
}
