import com.jimmoores.quandl.DataSetRequest;
import com.jimmoores.quandl.QuandlSession;
import com.jimmoores.quandl.Row;
import com.jimmoores.quandl.TabularResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import joinery.DataFrame;
import org.threeten.bp.LocalDate;

import static java.lang.Math.sqrt;
import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static void main2(String[] args) throws InterruptedException, ParseException, IOException {
        DateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS");
        List rows = new LinkedList();
        List columns = Arrays.asList("timestamp", "category", "name", "value");
        DataFrame df = new DataFrame(
                rows,
                columns,
                Arrays.asList(
                        Arrays.asList(
                                format.parse("20000101 00:00:04:010"),
                                format.parse("20000101 00:00:00:061"),
                                format.parse("20000101 00:00:02:000"),
                                format.parse("20000101 00:00:00:051"),
                                format.parse("20000101 00:00:00:121"),
                                format.parse("20000101 00:00:04:200"),
                                format.parse("20000101 00:00:01:121"),
                                format.parse("20000101 00:00:01:200")
                        ),
                        Arrays.asList("test", "test", "test", "beta", "beta", "beta", "beta", "beta"),
                        Arrays.asList("one", "two", "three", "one", "two", "three", "one", "two"),
                        Arrays.asList(70, 20, 60, 10, 30, 80, 40, 50)
                )
        );

        DataFrame indexed_df = df.reindex("timestamp");
        logger.info("result indexed:\n" + asString(indexed_df));

        String samplingColumnName = "timestamp";
        df = df.sortBy(samplingColumnName);
        DataFrame firstRow = df.head(0);
        Date firstDate = (Date)firstRow.get(0, "timestamp");
        logger.info("first date" +  firstDate);

        logger.info("result:\n" + asString(df));
        for(Object fields: df){
            Object field = ((List)fields).get(0);
            Date ts = (Date)field;
            logger.info(format.format(ts));
        }
    }

    public static void main(String[] args){
        QuandlSession session = QuandlSession.create();
        DataSetRequest.Builder requestBuilder = DataSetRequest.Builder.of("ECB/EURCHF").withMaxRows(200);
        TabularResult tabularResult = session.getDataSet(requestBuilder.build());
        Collection<String> columnNames = tabularResult.getHeaderDefinition().getColumnNames();
        DataFrame<Double> dataFrame = new DataFrame<>(columnNames);
        for(Row row: tabularResult){
            LocalDate date = row.getLocalDate("Date");
            Double value = row.getDouble("Value");
            Calendar calendar = new GregorianCalendar(date.getYear(), date.getMonthValue() + 1, date.getDayOfMonth());
            dataFrame.append(calendar.getTime(), Arrays.asList(new Double[]{value}));
        }
        Double stddev = dataFrame.percentChange().stddev().get(0, 1)/ sqrt(24*60*60);
        logger.info("loaded: " + stddev);
    }

    private static void processRecordedTicks(List<String> dirs, Integer ibCode) {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelOrderBookLevelOne(ibCode));
        logger.info("accessing storage: " + productStorage);
        if(Files.exists(productStorage)) {
            Map<String, Path> tickFiles = new TreeMap<>();
            try (Stream<Path> paths = Files.walk(productStorage)) {
                paths.forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        Pattern yyyymmddhhEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                        Matcher matcher = yyyymmddhhEnding.matcher(filePath.toString());
                        if(matcher.matches()) {
                            String yyyymmddhh = matcher.group(1) + " " + matcher.group(2);
                            logger.info("will be processing recorded ticks: " + yyyymmddhh);
                            tickFiles.put(yyyymmddhh, filePath);
                        }
                    }
                });
            } catch (IOException e) {
                logger.error("failed to access recorded ticks for product " + ibCode, e);
            }
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                String fullLine = null;
                try (Scanner scanner = new Scanner(filePath, "utf-8")) {
                    scanner.useDelimiter("\n");
                    while (scanner.hasNext()) {
                        String rawLine = scanner.next().trim();
                        if (!rawLine.equals("")){
                            fullLine = yyyymmddhh + ":" + rawLine;
                            String[] fields = fullLine.split(",");
                            Date timestamp = DATE_FORMAT.parse(fields[0] + "");
                            Integer volumeBid = Integer.valueOf(fields[1]);
                            BigDecimal priceBid = new BigDecimal(fields[2]);
                            BigDecimal priceAsk = new BigDecimal(fields[3]);
                            Integer volumeAsk = Integer.valueOf(fields[4]);
                            logger.info("line: " + timestamp + "," + volumeBid + "," + priceBid);
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

    private static String asString(DataFrame df) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        df.writeCsv(baos);
        baos.close();
        return baos.toString();
    }
}
