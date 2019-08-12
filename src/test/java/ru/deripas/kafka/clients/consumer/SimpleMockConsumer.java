package ru.deripas.kafka.clients.consumer;

import com.google.common.collect.Iterators;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SimpleMockConsumer<K, V> extends MockConsumer<K, V> {

    private final OffsetResetStrategy offsetResetStrategy;
    private Map<TopicPartition, AtomicLong> offsets;
    private ConsumerRebalanceListener listener;

    public SimpleMockConsumer() {
        this(OffsetResetStrategy.EARLIEST);
    }

    public SimpleMockConsumer(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
        this.offsetResetStrategy = offsetResetStrategy;
        offsets = new HashMap<>();
        listener = new NoOpConsumerRebalanceListener();
    }

    @Override
    public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        super.subscribe(topics, listener);
        this.listener = listener;
    }

    @Override
    public synchronized void rebalance(Collection<TopicPartition> newAssignment) {
        listener.onPartitionsRevoked(assignment());
        super.rebalance(newAssignment);
        switch (offsetResetStrategy) {
            case EARLIEST: {
                updateBeginningOffsets(newAssignment.stream().collect(Collectors.toMap(key -> key, key -> 0L)));
                break;
            }
            case LATEST: {
                updateEndOffsets(newAssignment.stream().collect(Collectors.toMap(key -> key, key -> 0L)));
                break;
            }
        }
        offsets = newAssignment.stream().collect(Collectors.toMap(key -> key, key -> new AtomicLong()));
        listener.onPartitionsAssigned(assignment());
    }

    public List<ConsumerRecord<K, V>> generateRecords(int count, Supplier<K> keySupplier, Supplier<V> valueSupplier) {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        Iterator<TopicPartition> iterator = Iterators.cycle(assignment());
        for (int i = 0; i < count; i++) {
            TopicPartition partition = iterator.next();
            AtomicLong offset = offsets.get(partition);
            ConsumerRecord<K, V> record = new ConsumerRecord<>(partition.topic(), partition.partition(), offset.incrementAndGet(), keySupplier.get(), valueSupplier.get());
            records.add(record);
            addRecord(record);
        }
        return records;
    }
}
