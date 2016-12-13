
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static rx.Observable.combineLatest;

class RandomWalk implements Func1<Boolean, Double> {
    private final static Logger logger = LoggerFactory.getLogger(RandomWalk.class);
    private final Double step;
    private Double state = 0.;

    RandomWalk(Double initial, Double step) {
        this.step = step;
        this.state = initial;
    }

    @Override
    public Double call(Boolean upOrDown) {
        if(upOrDown){
            logger.info("up one tick");
        } else {
            logger.info("down one tick");
        }
        state = upOrDown? state + step: state - step;
        return state;
    }

}

class Hysteresis implements Func1<Double, Double> {
    private final Double outputLow;
    private final Double outputHigh;
    private final Double thresholdLow;
    private final Double thresholdHigh;
    private Double state;

    Hysteresis(Double outputLow, Double outputHigh, Double thresholdLow, Double thresholdHigh) {
        this.outputLow = outputLow;
        this.outputHigh = outputHigh;
        this.thresholdLow = thresholdLow;
        this.thresholdHigh = thresholdHigh;
        state = outputLow;
    }

    @Override
    public Double call(Double value) {
        if(value >= thresholdHigh){
            state = outputHigh;
        } else if (value <= thresholdLow){
            state = outputLow;
        }
        return state;
    }
}

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);

    public static void main2(String[] args) throws InterruptedException {
        DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String value = "2016-11-14 10:53:59.260";
        System.out.println(LocalDateTime.parse(value, DATE_FORMAT));
    }

    public static void main3(String[] args) throws InterruptedException {
        Observable<Long> clock = Observable.interval(100, TimeUnit.MILLISECONDS, Schedulers.computation());

        final Random random = new Random();

        Double mean = 100.;
        Double step = 0.1;
        Observable<Double> randomWalk1 = clock
                .map(tick -> random.nextBoolean())
                .map(new RandomWalk(mean, step));

        Observable<Double> randomWalk2 = clock
                .map(tick -> random.nextBoolean())
                .map(new RandomWalk(mean, step));

        Observable<Double> signal = combineLatest(randomWalk1, randomWalk2, (value1, value2) -> value2 - value1);
        Observable<Double> filtered = signal.map(new Hysteresis(-1., 1., -1., 1.));

        signal.subscribe(value -> {
            logger.info("raw value = " + value);
        });

        filtered.subscribe(value -> {
            logger.info("filtered value = " + value);
        });
        sleep(10000);
    }

    public static void main(String[] args) throws InterruptedException {
        Double[] numbers = new Double[]{1.0, 2.0, 3.0};
        String[] letters = new String[]{"a", "b", "c", "d"};
        Observable<Double> stream1 = Observable.from(numbers);
        Observable<String> stream2 = Observable.from(letters);
        combineLatest(stream1.first(), stream2, (x, y) -> {
            logger.info("level1=" + x + ", " + y);
            return y;
        }).subscribe(x -> {
            logger.info("level2:" + x);
        });
    }

}
