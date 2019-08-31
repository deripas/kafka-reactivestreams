package ru.deripas.reactivestreams;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.deripas.reactivestreams.core.BaseProcessor;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class BProcessor<B extends Iterable<I>, I> extends BaseProcessor<B, I> {

    private final int prefetch;

    public static <B extends Iterable<I>, I> BProcessor<B, I> create() {
        return create(1);
    }

    public static <B extends Iterable<I>, I> BProcessor<B, I> create(int prefetch) {
        return new BProcessor<>(prefetch);
    }

    @Override
    protected void doOnRequest() {
        flush();
        long n = requests();
        if (n > 0) {
            subscription().request(n == Long.MAX_VALUE ? Long.MAX_VALUE : 1);
        }
    }

    @Override
    public void onNext(B batch) {
        log.info("onNext({})", batch);
//        batch.forEach(queue::offer);
        flush();
        long n = requests();
        if (n > 0) {
            subscription().request(n == Long.MAX_VALUE ? Long.MAX_VALUE : 1);
        }
    }

    private void flush() {
//        I item;
//        while (requests() > 0 && (item = queue.poll()) != null) {
//            log.info("fire onNext({})", item);
//            subscriber().onNext(item);
//        }
    }
}
