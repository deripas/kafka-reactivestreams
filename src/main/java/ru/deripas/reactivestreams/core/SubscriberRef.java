package ru.deripas.reactivestreams.core;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ru.deripas.reactivestreams.Subscribers;

import java.util.concurrent.atomic.AtomicReference;

public class SubscriberRef<T> implements Subscriber<T> {

    private final AtomicReference<Subscriber<? super T>> ref = new AtomicReference<>(Subscribers.empty());

    public void init(Subscriber<? super T> subscriber) {
        if (!ref.compareAndSet(Subscribers.empty(), subscriber)) {
            throw new IllegalStateException("Subscriber already init");
        }
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        ref.get().onSubscribe(subscription);
    }

    @Override
    public void onNext(T t) {
        ref.get().onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
        ref.get().onError(throwable);
    }

    @Override
    public void onComplete() {
        ref.get().onComplete();
    }
}
