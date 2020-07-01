package com.github.dao.kafka.clients.reactivestreams.consumer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static reactor.core.scheduler.Schedulers.parallel;
import static reactor.util.concurrent.Queues.XS_BUFFER_SIZE;

/**
 * @see https://stackoverflow.com/questions/54151419/prevent-flux-buffertimeout-from-overflowing-after-timeout
 */
@Slf4j
public class TestCase {

    private final BlockingQueue<Integer> queue;
    private final Flux<Integer> flux;
    private final BaseSubscriber<List<Integer>> batchSubscriber;

    public TestCase() {
        queue = new LinkedBlockingQueue<>();

        flux = Flux.generate(sink -> {
            try {
                sink.next(queue.poll(1, TimeUnit.DAYS));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        batchSubscriber = new BaseSubscriber<List<Integer>>() {
            protected void hookOnSubscribe(Subscription subscription) {
                // Don't request unbounded
            }

            protected void hookOnNext(List<Integer> value) {
                log.info("{}", value);
            }
        };
    }

    public void originalPipeline() {
        flux.subscribeOn(parallel())
                .log()
                .bufferTimeout(10, ofMillis(200))
                .subscribe(batchSubscriber);
    }


    public void uglyFix() {
        flux.subscribeOn(parallel())
                .log()
                .compose(onBackpressureBufferTimeoutWithRateLimit(10, ofMillis(200)))
                .subscribe(batchSubscriber);
    }

    @SneakyThrows
    public void fireTestData() {
        batchSubscriber.request(1);

        // Offer a partial batch of values
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        queue.offer(5);

        // Wait for timeout, expect [1, 2, 3, 4, 5] to be printed
        Thread.sleep(500);

        // Offer more values
        queue.offer(6);
        queue.offer(7);
        queue.offer(8);
        queue.offer(9);
        queue.offer(10);
        Thread.sleep(1000);
        batchSubscriber.request(1);
    }

    private static <T> Function<Flux<T>, Publisher<List<T>>> onBackpressureBufferTimeoutWithRateLimit(int maxSize, Duration maxTime) {
        return flux -> new Flux<List<T>>() {
            @Override
            public void subscribe(CoreSubscriber<? super List<T>> actual) {
                AtomicLong limit = new AtomicLong(maxSize * XS_BUFFER_SIZE);
                flux.compose(manualRateLimit(limit))
                        .bufferTimeout(maxSize, maxTime)
                        .onBackpressureBuffer()
                        .doOnNext(list -> limit.addAndGet(list.size()))
                        .subscribe(actual);
            }
        };
    }

    private static <T> Function<Flux<T>, Publisher<T>> manualRateLimit(AtomicLong requestsPending) {
        return flux -> new Flux<T>() {
            @Override
            public void subscribe(CoreSubscriber<? super T> actual) {
                flux.subscribe(new ManualRateLimitSubscriber<>(requestsPending, actual));
            }
        };
    }

    @Slf4j
    @RequiredArgsConstructor
    public static class ManualRateLimitSubscriber<T> extends BaseSubscriber<T> {

        private final AtomicLong requestsLimit;
        private final CoreSubscriber<? super T> actual;
        private final AtomicReference<Disposable> task = new AtomicReference<>();

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            actual.onSubscribe(new Subscription() {
                @Override
                public void request(long l) {
                    // ignore
                }

                @Override
                public void cancel() {
                    ManualRateLimitSubscriber.this.cancel();
                }
            });
            task.updateAndGet(current -> {
                if (current != null) current.dispose();
                return Schedulers.parallel()
                        .schedulePeriodically(() -> {
                            long requests = requestsLimit.getAndSet(0);
                            if (requests > 0) {
                                subscription.request(requests);
                            }
                        }, 0, 100, MILLISECONDS);
            });
        }

        @Override
        protected void hookOnNext(T value) {
            actual.onNext(value);
        }

        @Override
        protected void hookOnComplete() {
            actual.onComplete();
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        protected void hookFinally(SignalType type) {
            task.updateAndGet(current -> {
                if (current != null) current.dispose();
                return null;
            });
        }
    }

    public static void main(String[] args) {
        TestCase testCase = new TestCase();
//        testCase.originalPipeline(); // throw OverflowException: Could not emit buffer due to lack of requests
        testCase.uglyFix();     // ugly workaround with periodical call 'subscription.request(requests)'
        testCase.fireTestData();
    }
}
