package com.github.dao.kafka.clients.reactivestreams.consumer;

import com.github.dao.reactivestreams.AsyncTaskPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Reactive {@code Publisher<K, V>} for Kafka {@code Consumer<K, V>}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class AsyncConsumerPublisher<K, V> extends AsyncTaskPublisher<ConsumerRecords<K, V>> {

    public AsyncConsumerPublisher(Supplier<CompletionStage<ConsumerRecords<K, V>>> poll) {
        super(poll);
    }

    public static <K, V> AsyncConsumerPublisher<K, V> create(AsyncConsumer<K, V> asyncConsumer, Duration timeout) {
        return new AsyncConsumerPublisher<>(() -> asyncConsumer.poll(timeout));
    }
}
