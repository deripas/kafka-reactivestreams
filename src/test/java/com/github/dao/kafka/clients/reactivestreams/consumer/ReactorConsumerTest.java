package com.github.dao.kafka.clients.reactivestreams.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;

@Slf4j
public class ReactorConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;

    @BeforeMethod
    public void init() {
        mockConsumer = new SimpleMockConsumer<>();

        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));
    }

    @Test
    public void testSingle() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                        .to(Flux::from)
                        .take(1)
                        .flatMap(Flux::fromIterable))
                .expectSubscription()
                .expectNextSequence(records)
                .verifyComplete();
    }

    @Test
    public void testManyWithEmpty() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                        .to(Flux::from)
                        .take(3)
                        .map(ConsumerRecordsUtil::toList))
                .expectSubscription()
                .expectNext(records)
                .expectNext(Collections.emptyList())
                .expectNext(Collections.emptyList())
                .verifyComplete();
    }

    @Test
    public void testIgnoreEmpty() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                        .to(Flux::from)
                        .filter(consumerRecords -> !consumerRecords.isEmpty())
                        .timeout(Duration.ofMillis(200))
                        .map(ConsumerRecordsUtil::toList))
                .expectSubscription()
                .expectNext(records)
                .verifyError(TimeoutException.class);
    }

    @Test
    public void testPerMessageConsumer() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                ReactiveConsumer.poll(mockConsumer, Duration.ofSeconds(1))
                        .split(10)
                        .to(Flux::from)
                        .take(records.size()))
                .expectSubscription()
                .expectNextSequence(records)
                .verifyComplete();
    }
}
