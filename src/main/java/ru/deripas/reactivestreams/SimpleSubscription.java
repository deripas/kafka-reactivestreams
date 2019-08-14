package ru.deripas.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static ru.deripas.kafka.clients.util.AtomicLongUtil.safeAdd;

@Slf4j
@AllArgsConstructor
public class SimpleSubscription<T> implements Subscription {

    private final Subscriber<? super T> subscriber;
    private final Supplier<CompletionStage<T>> request;

    private final AtomicLong requests = new AtomicLong();
    private final AtomicBoolean isBusy = new AtomicBoolean();
    private final AtomicBoolean isCanceled = new AtomicBoolean();

    @Override
    public void request(long n) {
        log.info("request({})", n == Long.MAX_VALUE ? "unbound" : n);
        safeAdd(requests, n);
        tryRequest();
    }

    private void tryRequest() {
        if (isCanceled.get()) {
            log.info("request ignored, canceled");
            return;
        }
        if (requests.get() > 0 && isBusy.compareAndSet(false, true)) {
            log.info("start request");
            request.get().whenComplete(this::handleResult);
        }
    }

    @Override
    public void cancel() {
        if (isCanceled.compareAndSet(false, true)) {
            log.info("cancel()");
            subscriber.onComplete();
        }
    }

    private void handleResult(T result, Throwable throwable) {
        try {
            if (throwable != null) {
                log.error("request error", throwable);
                subscriber.onError(throwable);
            } else {
                log.info("response: {}", result);
                requests.decrementAndGet();
                subscriber.onNext(result);
            }
        } finally {
            isBusy.set(false);
            tryRequest();
        }
    }
}
