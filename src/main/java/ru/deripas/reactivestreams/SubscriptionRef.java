package ru.deripas.reactivestreams;

import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionRef implements Subscription {

    private final AtomicReference<Subscription> ref = new AtomicReference<>(Subscriptions.empty());

    public void init(Subscription subscription) {
        if (!ref.compareAndSet(Subscriptions.empty(), subscription)) {
            throw new IllegalStateException("Subscription already init");
        }
    }

    @Override
    public void request(long n) {
        ref.get().request(n);
    }

    @Override
    public void cancel() {
        ref.get().cancel();
    }
}
