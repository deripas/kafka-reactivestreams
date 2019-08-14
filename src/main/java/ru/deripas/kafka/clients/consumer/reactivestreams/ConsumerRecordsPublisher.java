package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;
import ru.deripas.reactivestreams.SimplePublisher;
import ru.deripas.reactivestreams.SimpleSubscription;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor
public class ConsumerRecordsPublisher<K, V> extends SimplePublisher<ConsumerRecords<K, V>> {

    private final Supplier<CompletionStage<ConsumerRecords<K, V>>> poll;

    public static <K, V> ConsumerRecordsPublisher<K, V> create(AsyncConsumer<K, V> asyncConsumer, Duration timeout) {
        return new ConsumerRecordsPublisher<>(() -> asyncConsumer.poll(timeout));
    }

    @Override
    protected Subscription createSubscription(Subscriber<? super ConsumerRecords<K, V>> subscriber) {
        return new SimpleSubscription<>(subscriber, poll);
    }
}
