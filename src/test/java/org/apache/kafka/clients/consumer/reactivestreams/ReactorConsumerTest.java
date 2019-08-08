package org.apache.kafka.clients.consumer.reactivestreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.SimpleMockConsumer;
import org.apache.kafka.clients.consumer.async.AsyncConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Collections.singleton;

@Slf4j
public class ReactorConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;
    private AsyncConsumer<Integer, Integer> asyncConsumer;

    @BeforeEach
    public void init() {
        mockConsumer = new SimpleMockConsumer<>();
        asyncConsumer = new AsyncConsumer<>(mockConsumer);
    }

    @Test
    public void test() {
        TopicPartition partition = new TopicPartition("test", 0);
        mockConsumer.subscribe(singleton(partition.topic()));
        mockConsumer.rebalance(singleton(partition));
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                Flux.from(new ConsumerRecordsPublisher<>(asyncConsumer))
                        .limitRate(1)
                        .take(1)
                        .flatMap(Flux::fromIterable))
                .expectSubscription()
                .expectNextSequence(records)
                .verifyComplete();
    }
}
