package ru.deripas.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.deripas.reactivestreams.core.BaseProcessor;

import java.util.function.Function;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class FilteringProcessor<T> extends BaseProcessor<T, T> {

    private final Function<T, Boolean> filter;

    @Override
    public void onNext(T item) {
        if (filter.apply(item)) {
            log.info("ignore item, try again");
            subscription().request(1);
        } else {
            subscriber().onNext(item);
        }
    }
}
