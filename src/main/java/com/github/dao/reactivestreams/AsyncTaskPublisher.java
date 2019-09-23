package com.github.dao.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.github.dao.reactivestreams.core.BasePublisher;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor
public class AsyncTaskPublisher<T> extends BasePublisher<T> {

    private final Supplier<CompletionStage<T>> task;
    private final AtomicBoolean isBusy = new AtomicBoolean();

    @Override
    protected void doOnRequest() {
        if (needMore() && isBusy.compareAndSet(false, true)) {
            log.info("start request task");
            task.get().whenComplete(this::handleResult);
        }
    }

    private void handleResult(T result, Throwable throwable) {
        try {
            if (throwable != null) {
                log.error("request error", throwable);
                subscriber().onError(throwable);
            } else {
                log.info("response: {}", result);
                subscriber().onNext(result);
            }
        } finally {
            isBusy.set(false);
            doOnRequest();
        }
    }
}
