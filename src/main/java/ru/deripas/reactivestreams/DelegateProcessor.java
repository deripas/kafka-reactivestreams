package ru.deripas.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class DelegateProcessor<T> extends SimplePublisher<T> implements Processor<T, T> {

    private final SubscriptionRef ref = new SubscriptionRef();

    protected Subscription subscription() {
        return ref;
    }

    @Override
    protected Subscription createSubscription(Subscriber<? super T> subscriber) {
        return ref;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        ref.init(subscription);
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
