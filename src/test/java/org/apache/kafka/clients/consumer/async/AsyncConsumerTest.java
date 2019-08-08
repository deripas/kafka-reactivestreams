package org.apache.kafka.clients.consumer.async;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.SimpleMockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;

public class AsyncConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;
    private AsyncConsumer<Integer, Integer> asyncConsumer;

    @BeforeEach
    public void init() {
        mockConsumer = new SimpleMockConsumer<>();
        asyncConsumer = new AsyncConsumer<>(mockConsumer);
    }

    @Test
    public void testSimple() throws ExecutionException, InterruptedException {
        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));

        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);
        List<ConsumerRecord<Integer, Integer>> result = toList(asyncConsumer
                .doOnConsumer(consumer -> consumer.poll(Duration.ofSeconds(5)))
                .get());

        Assertions.assertEquals(records, result);
    }

    @Test
    public void testError() {
        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));
        mockConsumer.setException(new TimeoutException());

        CompletableFuture<ConsumerRecords<Integer, Integer>> future = asyncConsumer.doOnConsumer(consumer -> consumer.poll(Duration.ofSeconds(5)));
        Assertions.assertThrows(ExecutionException.class, future::get);
    }

    private List<ConsumerRecord<Integer, Integer>> toList(ConsumerRecords<Integer, Integer> consumerRecords) {
        return StreamSupport.stream(consumerRecords.spliterator(), false).collect(Collectors.toList());
    }
}
