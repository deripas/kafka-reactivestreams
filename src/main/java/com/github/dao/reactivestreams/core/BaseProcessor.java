package com.github.dao.reactivestreams.core;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscription;

public abstract class BaseProcessor<IN, OUT> extends BasePublisher<OUT> implements Processor<IN, OUT> {

    private final SubscriptionRef ref = new SubscriptionRef();

    protected Subscription subscription() {
        return ref;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        ref.init(subscription);
    }

    @Override
    public void onError(Throwable t) {
        subscription().cancel();
        subscriber().onError(t);
    }

    @Override
    public void onComplete() {
        subscription().cancel();
        subscriber().onComplete();
    }

    @Override
    protected void doOnRequest() {
        fireRequest(requests());
    }

    protected void fireRequest(long n) {
        if (n > 0) {
            subscription().request(n);
        }
    }
}
