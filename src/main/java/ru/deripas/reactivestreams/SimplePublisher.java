package ru.deripas.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public abstract class SimplePublisher<T> implements Publisher<T> {

    private final SubscriberRef<T> ref = new SubscriberRef<>();

    protected Subscriber<? super T> subscriber() {
        return ref;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        ref.init(subscriber);
        subscriber.onSubscribe(createSubscription(subscriber));
    }

    protected abstract Subscription createSubscription(Subscriber<? super T> subscriber);

    public <R> Publisher<R> with(Processor<T, R> processor) {
        subscribe(processor);
        return processor;
    }
}
