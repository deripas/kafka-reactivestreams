package ru.deripas.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.reactivestreams.Subscription;

@UtilityClass
public class Subscriptions {

    private static final Subscription NULL = error(new IllegalStateException("Subscription not init yet"));

    public static Subscription empty() {
        return NULL;
    }

    public static Subscription error(Exception error) {
        return new SubscriptionThrows(error);
    }

    @AllArgsConstructor
    private class SubscriptionThrows implements Subscription {

        private final Exception error;

        @SneakyThrows
        @Override
        public void request(long l) {
            throw error;
        }

        @SneakyThrows
        @Override
        public void cancel() {
            throw error;
        }
    }
}