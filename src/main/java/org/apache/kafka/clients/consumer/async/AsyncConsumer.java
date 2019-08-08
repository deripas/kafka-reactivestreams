package org.apache.kafka.clients.consumer.async;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.supplyAsync;

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
}
