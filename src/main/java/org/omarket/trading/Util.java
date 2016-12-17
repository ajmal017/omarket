package org.omarket.trading;

import rx.Observable;

import static rx.Observable.combineLatest;

/**
 * Created by Christophe on 17/12/2016.
 */
public class Util {
    /**
     * Waits for emission of all elements from stream1 before emitting elements from stream2.
     * Similar to concat but with no need for both streams to return identical types.
     *
     *     Observable<Integer> stream1 = Observable.from(new Integer[]{1,2,3,4});
     *     Observable<String> stream2 = Observable.from(new String[]{"a","b","c","d", "e"});
     *     chain(stream1.doOnNext(System.out::println), stream2).subscribe(System.out::println);
     *     > 1
     *     > 2
     *     > 3
     *     > 4
     *     > a
     *     > b
     *     > c
     *     > d
     *     > e
     *
     * @param stream1
     * @param stream2
     * @param <T1>
     * @param <T2>
     * @return Observable emitting elements from stream2 only after stream1 completion
     */
    public static <T1, T2> Observable<T2> chain(Observable<T1> stream1, Observable<T2> stream2) {
        Observable<T1> last = stream1
                .last();
        return combineLatest(last, stream2, (x, y) -> y);
    }
}
