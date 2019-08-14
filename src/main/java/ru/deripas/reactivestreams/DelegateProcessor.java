package ru.deripas.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

public class DelegateProcessor<T> extends SimplePublisher<T> implements Processor<T, T> {

    private final AtomicReference<Subscription> subscription = new AtomicReference<>(null);

    protected Subscription subscription() {
        return subscription.get();
    }

    @Override
    protected Subscription createSubscription(Subscriber<? super T> subscriber) {
        return subscription();
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        if (!this.subscription.compareAndSet(null, subscription)) {
            throw new IllegalStateException("supported only single subscription!");
        }
    }

    @Override
    public void onNext(T t) {
        subscriber().onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
        subscriber().onError(throwable);
    }

    @Override
    public void onComplete() {
        subscriber().onComplete();
    }
}
