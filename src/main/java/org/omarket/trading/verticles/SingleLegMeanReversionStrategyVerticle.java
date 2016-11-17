package org.omarket.trading.verticles;

import com.jimmoores.quandl.DataSetRequest;
import com.jimmoores.quandl.QuandlSession;
import com.jimmoores.quandl.Row;
import com.jimmoores.quandl.TabularResult;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import joinery.DataFrame;
import org.omarket.trading.OrderBookLevelOneImmutable;
import org.threeten.bp.LocalDate;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.Math.sqrt;
import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);
    private static JsonObject contract;
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
    public static final String ADDRESS_STRATEGY_SIGNAL = "oot.strategy.signal.singleLeg";
    private static OrderBookLevelOneImmutable orderBookPrev;
    private static OrderBookLevelOneImmutable orderBook;
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private void processRecordedOrderBooks(List<String> dirs, Integer ibCode) {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelOrderBookLevelOne(ibCode));
        logger.info("accessing storage: " + productStorage);
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = new TreeMap<>();
            try (Stream<Path> paths = Files.walk(productStorage)) {
                paths.forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        Pattern yyyymmddhhEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                        Matcher matcher = yyyymmddhhEnding.matcher(filePath.toString());
                        if (matcher.matches()) {
                            String yyyymmddhh = matcher.group(1) + " " + matcher.group(2);
                            logger.info("will be processing recorded ticks: " + yyyymmddhh);
                            tickFiles.put(yyyymmddhh, filePath);
                        }
                    }
                });
            } catch (IOException e) {
                logger.error("failed to access recorded ticks for product " + ibCode, e);
            }
            DataFrame<Double> eurchfDaily = loadQuandlInstrument("ECB/EURCHF", 200);
            Double stddev = eurchfDaily.percentChange().stddev().get(0, 1)/ sqrt(24*60*60);
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                String fullLine = null;

                try (Scanner scanner = new Scanner(filePath, "utf-8")) {
                    scanner.useDelimiter("\n");
                    while (scanner.hasNext()) {
                        String rawLine = scanner.next().trim();
                        if (!rawLine.equals("")) {
                            fullLine = yyyymmddhh + ":" + rawLine;
                            String[] fields = fullLine.split(",");
                            Date timestamp = DATE_FORMAT.parse(fields[0] + "");
                            Integer volumeBid = Integer.valueOf(fields[1]);
                            BigDecimal priceBid = new BigDecimal(fields[2]);
                            BigDecimal priceAsk = new BigDecimal(fields[3]);
                            Integer volumeAsk = Integer.valueOf(fields[4]);
                            DateFormat isoFormat = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
                            orderBook = new OrderBookLevelOneImmutable(timestamp, volumeBid, priceBid, priceAsk, volumeAsk);
                            logger.info("current order book: " + orderBook + " (" + isoFormat.format(orderBook.getLastModified()) + ")");
                            if (orderBookPrev != null && !orderBook.sameSampledTime(orderBookPrev, OrderBookLevelOneImmutable.Sampling.SECOND)){
                                processOrderBook(orderBookPrev, stddev, true);
                            }
                            orderBookPrev = orderBook;
                        }
                    }
                    scanner.close();
                } catch (IOException e) {
                    logger.error("unable to access tick file: " + filePath, e);
                } catch (ParseException e) {
                    logger.error("unable to parse line: " + fullLine, e);
                }
            }
        }
    }

    private void processOrderBook(OrderBookLevelOneImmutable orderBook, Double stddev, boolean isBacktest) {
        BigDecimal midPrice = orderBook.getBestBidPrice().add(orderBook.getBestAskPrice()).divide(BigDecimal.valueOf(2));
        vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, new JsonObject().put("signal", midPrice.doubleValue()));
    }

    private static DataFrame<Double> loadQuandlInstrument(String quandlCode, int samples) {
        QuandlSession session = QuandlSession.create();
        DataSetRequest.Builder requestBuilder = DataSetRequest.Builder.of(quandlCode).withMaxRows(samples);
        TabularResult tabularResult = session.getDataSet(requestBuilder.build());
        Collection<String> columnNames = tabularResult.getHeaderDefinition().getColumnNames();
        DataFrame<Double> dataFrame = new DataFrame<>(columnNames);
        for(Row row: tabularResult){
            LocalDate date = row.getLocalDate("Date");
            Double value = row.getDouble("Value");
            Calendar calendar = new GregorianCalendar(date.getYear(), date.getMonthValue() + 1, date.getDayOfMonth());
            dataFrame.append(calendar.getTime(), Arrays.asList(new Double[]{value}));
        }
        return dataFrame;
    }

    public void start() {
        logger.info("starting single leg mean reversion strategy verticle");
        final Integer ibCode = 12087817;

        vertx.executeBlocking(future -> {
            JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
            List<String> dirs = storageDirs.getList();
            processRecordedOrderBooks(dirs, ibCode);
            future.succeeded();
        }, result -> {
            logger.info("processed recorded ticks for " + ibCode);
            // now launching realtime ticks
            MarketDataVerticle.subscribeProduct(vertx, ibCode, subscribeProductResult -> {
                if (subscribeProductResult.succeeded()) {
                    vertx.setPeriodic(1000, id -> {
                        // Sampling: calculates signal
                        contract = subscribeProductResult.result().body();
                        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
                        logger.info("order book for " + symbol + ": " + orderBook);
                    });

                    // Constantly maintains order book up to date
                    final String channelProduct = createChannelOrderBookLevelOne(ibCode);
                    vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> {
                        try {
                            orderBook = OrderBookLevelOneImmutable.fromJSON(message.body());
                        } catch (ParseException e) {
                            logger.error("failed to parse tick data for contract " + contract, e);
                        }
                    });

                } else {
                    logger.error("failed to subscribe to: " + ibCode);
                }
            });
        });

    }

}
