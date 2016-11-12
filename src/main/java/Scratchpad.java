import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import joinery.DataFrame;

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);

    public static void main(String[] args) throws InterruptedException, ParseException, IOException {
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

    private static String asString(DataFrame df) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        df.writeCsv(baos);
        baos.close();
        return baos.toString();
    }
}
