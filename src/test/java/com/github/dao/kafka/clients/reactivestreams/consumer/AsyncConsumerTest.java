package com.github.dao.kafka.clients.reactivestreams.consumer;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.github.dao.kafka.clients.reactivestreams.consumer.ConsumerRecordsUtil.toList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;

public class AsyncConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;
    private AsyncConsumer<Integer, Integer> asyncConsumer;

    @BeforeMethod
    public void init() {
        mockConsumer = new SimpleMockConsumer<>();
        asyncConsumer = new AsyncConsumer<>(mockConsumer);

        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));
    }

    @Test
    public void testSimple() throws ExecutionException, InterruptedException {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);
        List<ConsumerRecord<Integer, Integer>> result = toList(asyncConsumer
                .poll(Duration.ofSeconds(5))
                .get());

        assertEquals(records, result);
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testError() throws ExecutionException, InterruptedException {
        mockConsumer.setException(new TimeoutException());
        asyncConsumer.poll(Duration.ofSeconds(5)).get();
    }
}
