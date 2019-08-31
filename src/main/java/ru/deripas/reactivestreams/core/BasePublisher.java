package ru.deripas.reactivestreams.core;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static ru.deripas.reactivestreams.util.RequestUtil.safeAdd;

@Slf4j
public abstract class BasePublisher<T> implements Publisher<T> {

    private final SubscriberRef<T> ref = new SubscriberRef<>();
    private final AtomicLong requests = new AtomicLong();
    private final AtomicBoolean isCanceled = new AtomicBoolean();

    protected Subscriber<? super T> subscriber() {
        return ref;
    }

    public long requests() {
        return requests.get();
    }

    public boolean isUnbound() {
        return requests() == Long.MAX_VALUE;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        ref.init(new DelegateSubscriber<T>(subscriber) {
            @Override
            public void onNext(T item) {
                safeAdd(requests, -1);
                super.onNext(item);
            }
        });
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                if (isCanceled.get()) {
                    log.info("request({}) ignored, canceled", n == Long.MAX_VALUE ? "unbound" : n);
                    return;
                }
                log.info("request({})", n == Long.MAX_VALUE ? "unbound" : n);
                safeAdd(requests, n);
                doOnRequest();
            }

            @Override
            public void cancel() {
                if (isCanceled.compareAndSet(false, true)) {
                    log.info("cancel()");
                    doOnCancel();
                }
            }
        });
    }

    protected abstract void doOnRequest();

    protected void doOnCancel() {
        // no action yet
    }
}