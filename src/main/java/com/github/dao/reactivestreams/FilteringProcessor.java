package com.github.dao.reactivestreams;

import com.github.dao.reactivestreams.core.BaseProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class FilteringProcessor<T> extends BaseProcessor<T, T> {

    private final Function<T, Boolean> filter;

    @Override
    public void onNext(T item) {
        if (filter.apply(item)) {
            log.info("ignore item, try again");
            fireRequest(1);
        } else {
            subscriber().onNext(item);
        }
    }
}
