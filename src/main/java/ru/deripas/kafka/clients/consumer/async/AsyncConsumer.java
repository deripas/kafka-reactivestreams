package ru.deripas.kafka.clients.consumer.async;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

    private final Consumer<K, V> consumer;
    private final ScheduledExecutorService executorService;

    public AsyncConsumer(Consumer<K, V> consumer) {
        this.consumer = consumer;
        executorService = Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder()
                .namingPattern("kafka-consumer-%d")
                .build());
    }

    public <T> CompletableFuture<T> doOnConsumer(Function<Consumer<K, V>, T> transform) {
        return supplyAsync(() -> transform.apply(consumer), executorService);
    }

    public CompletableFuture<ConsumerRecords<K, V>> poll(Duration timeout) {
        return doOnConsumer(consumer -> consumer.poll(timeout));
    }
}
