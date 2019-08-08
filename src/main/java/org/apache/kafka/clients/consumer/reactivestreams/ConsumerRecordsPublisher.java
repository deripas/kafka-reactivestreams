package org.apache.kafka.clients.consumer.reactivestreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.async.AsyncConsumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ConsumerRecordsPublisher<K, V> implements Publisher<ConsumerRecords<K, V>>, Subscription {

    private final AsyncConsumer<K, V> asyncConsumer;
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong poll = new AtomicLong();
    private final AtomicBoolean cancel = new AtomicBoolean();
    private final AtomicReference<Subscriber<? super ConsumerRecords<K, V>>> reference = new AtomicReference<Subscriber<? super ConsumerRecords<K, V>>>();

    public ConsumerRecordsPublisher(AsyncConsumer<K, V> asyncConsumer) {
        this.asyncConsumer = asyncConsumer;
    }

    private Subscriber<? super ConsumerRecords<K, V>> subscriber() {
        return reference.get();
    }

    public void subscribe(Subscriber<? super ConsumerRecords<K, V>> subscriber) {
        if (!reference.compareAndSet(null, subscriber)) {
            throw new IllegalStateException("supported only single subscriber!");
        }
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        log.info("request({})", n == Long.MAX_VALUE ? "unbound" : n);
        requests.updateAndGet(val -> {
            if (val == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long r = val + n;
            if (r <= 0) {
                return Long.MAX_VALUE;
            }
            return r;
        });
        schedulePoll();
    }

    @Override
    public void cancel() {
        log.info("cancel()");
        if (cancel.compareAndSet(false, true)) {
            subscriber().onComplete();
        }
    }

    private void schedulePoll() {
        asyncConsumer.doOnConsumer(consumer -> consumer.poll(Duration.ofSeconds(1)))
                .whenComplete((consumerRecords, throwable) -> {
                    if (throwable != null) {
                        subscriber().onError(throwable);
                        return;
                    }
                    if (!consumerRecords.isEmpty()) {
                        subscriber().onNext(consumerRecords);
                    }
                    if (requests.decrementAndGet() > 0) {
                        schedulePoll();
                    }
                });
    }
}
