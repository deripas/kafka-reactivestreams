package com.github.dao.kafka.clients.reactivestreams.consumer;

import com.github.dao.kafka.clients.reactivestreams.consumer.internal.Poll;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;

public class ReactiveConsumer {

    public static <K, V> Poll<K, V> poll(Consumer<K, V> consumer, Duration timeout) {
        return new Poll<>(new AsyncConsumer<>(consumer), timeout);
    }
}
