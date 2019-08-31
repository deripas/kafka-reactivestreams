package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;
import ru.deripas.reactivestreams.AsyncTaskPublisher;

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
public class ConsumerRecordsPublisher<K, V> extends AsyncTaskPublisher<ConsumerRecords<K, V>> {

    public ConsumerRecordsPublisher(Supplier<CompletionStage<ConsumerRecords<K, V>>> poll) {
        super(poll);
    }

    public static <K, V> ConsumerRecordsPublisher<K, V> create(AsyncConsumer<K, V> asyncConsumer, Duration timeout) {
        return new ConsumerRecordsPublisher<>(() -> asyncConsumer.poll(timeout));
    }
}
