package com.github.dao.reactivestreams.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import java.util.function.Supplier;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PublisherBuilder<T> {

    private final Supplier<Publisher<T>> source;

    public static <T> PublisherBuilder<T> create(Publisher<T> source) {
        return create(() -> source);
    }

    public static <T> PublisherBuilder<T> create(Supplier<Publisher<T>> source) {
        return new PublisherBuilder<>(source);
    }

    public <R> PublisherBuilder<R> then(Processor<T, R> processor) {
        return new PublisherBuilder<>(() -> {
            build().subscribe(processor);
            return processor;
        });
    }

    public Publisher<T> build() {
        return source.get();
    }
}
