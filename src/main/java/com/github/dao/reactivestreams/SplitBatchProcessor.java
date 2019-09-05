package com.github.dao.reactivestreams;

import com.github.dao.reactivestreams.core.BaseProcessor;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SplitBatchProcessor<B extends Iterable<I>, I> extends BaseProcessor<B, I> {

    private final int prefetch;

    public static <B extends Iterable<I>, I> SplitBatchProcessor<B, I> create() {
        return create(1);
    }

    public static <B extends Iterable<I>, I> SplitBatchProcessor<B, I> create(int prefetch) {
        return new SplitBatchProcessor<>(prefetch);
    }

    @Override
    protected void doOnRequest() {
        subscription().request(needUnbounded() ? Long.MAX_VALUE : prefetch);
    }

    @Override
    public void onNext(B batch) {
        log.info("onNext({})", batch);
        batch.forEach(subscriber()::onNext);
        if (needMore()) {
            fireRequest(prefetch);
        }
    }
}
