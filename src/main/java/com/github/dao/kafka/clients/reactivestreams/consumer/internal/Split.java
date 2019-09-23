package com.github.dao.kafka.clients.reactivestreams.consumer.internal;

import com.github.dao.reactivestreams.BackPressureBufferProcessor;
import com.github.dao.reactivestreams.FromIterableProcessor;
import com.github.dao.reactivestreams.util.PublisherBuilder;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Publisher;

import java.util.function.Function;

@AllArgsConstructor
public class Split<K, V> {

    private final PublisherBuilder<ConsumerRecords<K, V>> builder;
    private final int capacity;

    public <R> R to(Function<Publisher<ConsumerRecord<K, V>>, R> function) {
        Publisher<ConsumerRecord<K, V>> publisher = builder
                .then(new FromIterableProcessor<>())
                .then(BackPressureBufferProcessor.create(capacity))
                .build();
        return function.apply(publisher);
    }
}
