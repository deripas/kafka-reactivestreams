package org.apache.kafka.reactivestreams.clients.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.reactivestreams.clients.consumer.ConsumerConfig;

import java.util.function.Function;

public interface AsyncConsumerFactory<K, V> extends Function<ConsumerConfig<K, V>, Consumer<K, V>> {
}
