package com.github.dao.reactivestreams;

import com.github.dao.reactivestreams.core.BaseProcessor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FromIterableProcessor<B extends Iterable<I>, I> extends BaseProcessor<B, I> {

    public static <B extends Iterable<I>, I> FromIterableProcessor<B, I> create() {
        return new FromIterableProcessor<>();
    }

    @Override
    protected void doOnRequest() {
        fireRequest(needUnbounded() ? Long.MAX_VALUE : 1);
    }

    @Override
    public void onNext(B batch) {
        log.info("onNext({})", batch);
        batch.forEach(subscriber()::onNext);
        if (needMore()) {
            fireRequest(1);
        }
    }
}
