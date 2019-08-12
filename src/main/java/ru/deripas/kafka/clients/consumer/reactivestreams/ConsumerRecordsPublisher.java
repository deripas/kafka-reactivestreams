package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

@Slf4j
public class ConsumerRecordsPublisher<K, V> extends SimplePublisher<ConsumerRecords<K, V>> {

    public ConsumerRecordsPublisher(Supplier<CompletionStage<ConsumerRecords<K, V>>> poll) {
        super(subscriber -> new SimpleSubscription<>(subscriber, poll));
    }

    public static <K, V> ConsumerRecordsPublisher<K, V> create(AsyncConsumer<K, V> asyncConsumer) {
        return new ConsumerRecordsPublisher<>(() -> asyncConsumer.poll(Duration.ofSeconds(1)));
    }
}
