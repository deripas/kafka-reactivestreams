package com.github.dao.kafka.clients.reactivestreams.consumer;

import io.reactivex.Flowable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;

@Slf4j
public class RxConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;

    @BeforeMethod
    public void init() {
        mockConsumer = new SimpleMockConsumer<>();

        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));
    }

    @Test
    public void test() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                .to(Flowable::fromPublisher)
                .take(1)
                .flatMap(Flowable::fromIterable)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValueSequence(records)
                .assertNoErrors()
                .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testManyWithEmpty() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                .to(Flowable::fromPublisher)
                .take(3)
                .map(ConsumerRecordsUtil::toList)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValues(records, Collections.emptyList(), Collections.emptyList())
                .assertNoErrors()
                .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testIgnoreEmpty() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                .to(Flowable::fromPublisher)
                .filter(consumerRecords -> !consumerRecords.isEmpty())
                .timeout(200, TimeUnit.MILLISECONDS)
                .map(ConsumerRecordsUtil::toList)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValues(records)
                .assertError(TimeoutException.class);
    }

    @Test
    public void testPerMessageConsumer() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                .split(10)
                .to(Flowable::fromPublisher)
                .take(records.size())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValueSequence(records)
                .assertNoErrors()
                .assertComplete();
    }
}
