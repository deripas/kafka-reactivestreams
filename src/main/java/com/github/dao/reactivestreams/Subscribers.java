package com.github.dao.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@UtilityClass
public class Subscribers {

    private static final Subscriber NULL = error(new IllegalStateException("Subscriber not init yet!"));

    @SuppressWarnings("unchecked")
    public static <T> Subscriber<T> empty() {
        return NULL;
    }

    public static <T> Subscriber<T> error(Exception error) {
        return new SubscriberThrows<>(error);
    }

    @AllArgsConstructor
    private static class SubscriberThrows<T> implements Subscriber<T> {

        private final Exception error;

        @SneakyThrows
        @Override
        public void onSubscribe(Subscription subscription) {
            throw error;
        }

        @SneakyThrows
        @Override
        public void onNext(T t) {
            throw error;
        }

        @SneakyThrows
        @Override
        public void onError(Throwable throwable) {
            throw error;
        }

        @SneakyThrows
        @Override
        public void onComplete() {
            throw error;
        }
    }
}
