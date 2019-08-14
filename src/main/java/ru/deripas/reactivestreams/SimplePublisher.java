package ru.deripas.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

public abstract class SimplePublisher<T> implements Publisher<T> {

    private final AtomicReference<Subscriber<? super T>> subscriber = new AtomicReference<>(null);

    protected Subscriber<? super T> subscriber() {
        return subscriber.get();
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        if (!this.subscriber.compareAndSet(null, subscriber)) {
            throw new IllegalStateException("supported only single subscriber!");
        }
        subscriber.onSubscribe(createSubscription(subscriber));
    }

    protected abstract Subscription createSubscription(Subscriber<? super T> subscriber);
}
