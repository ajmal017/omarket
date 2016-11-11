import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);

    public static void main(String[] args) throws InterruptedException {
        DataFrame<Object> df = new DataFrame<Object>(
                Arrays.<Object>asList("row1", "row2", "row3"),
                Arrays.<Object>asList("category", "name", "value"),
                Arrays.<List<Object>>asList(
                        Arrays.<Object>asList("test", "test", "test", "beta", "beta", "beta"),
                        Arrays.<Object>asList("one", "two", "three", "one", "two", "three" ),
                        Arrays.<Object>asList(10, 20, 30, 40, 50, 60)
                )
        );

    }
}
