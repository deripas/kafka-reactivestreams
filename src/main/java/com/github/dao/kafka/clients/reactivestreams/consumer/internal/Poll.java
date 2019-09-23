package com.github.dao.kafka.clients.reactivestreams.consumer.internal;


import com.github.dao.kafka.clients.reactivestreams.consumer.AsyncConsumer;
import com.github.dao.kafka.clients.reactivestreams.consumer.AsyncConsumerPublisher;
import com.github.dao.reactivestreams.util.PublisherBuilder;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.function.Function;

@AllArgsConstructor
public class Poll<K, V> {

    private final AsyncConsumer<K, V> consumer;
    private final Duration timeout;

    private PublisherBuilder<ConsumerRecords<K, V>> toBuilder() {
        return PublisherBuilder.create(AsyncConsumerPublisher.create(consumer, timeout));
    }

    public <R> R to(Function<Publisher<ConsumerRecords<K, V>>, R> function) {
        return function.apply(toBuilder().build());
    }

    public Split<K, V> split(int capacity) {
        return new Split<>(toBuilder(), capacity);
    }
}