package com.github.dao.kafka.clients.reactivestreams.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Thread safe wrapper for Kafka {@code Consumer<K, V>}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class AsyncConsumer<K, V> {

    public static final ThreadFactory THREAD_FACTORY = new BasicThreadFactory.Builder()
            .namingPattern("kafka-consumer-%d")
            .build();

    private final Consumer<K, V> consumer;
    private final ScheduledExecutorService executorService;

    public AsyncConsumer(Consumer<K, V> consumer) {
        this(consumer, THREAD_FACTORY);
    }

    public AsyncConsumer(Consumer<K, V> consumer, ThreadFactory factory) {
        this.consumer = consumer;
        executorService = Executors.newSingleThreadScheduledExecutor(factory);
    }

    public <T> CompletableFuture<T> doOnConsumer(Function<Consumer<K, V>, T> transform) {
        return supplyAsync(() -> transform.apply(consumer), executorService);
    }

    public CompletableFuture<ConsumerRecords<K, V>> poll(Duration timeout) {
        return doOnConsumer(consumer -> consumer.poll(timeout));
    }
}
