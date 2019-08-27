package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import ru.deripas.kafka.clients.consumer.ConsumerRecordsUtil;
import ru.deripas.kafka.clients.consumer.SimpleMockConsumer;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;
import ru.deripas.reactivestreams.FilteringProcessor;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;
import static java.util.stream.StreamSupport.stream;

@Slf4j
public class ReactorConsumerTest {

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
    public void testSingle() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        StepVerifier.create(
                Flux.from(ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1)))
                        .take(1)
                        .flatMap(Flux::fromIterable))
                .expectSubscription()
                .expectNextSequence(records)
                .verifyComplete();
    }

    @Test
    public void testManyWithEmpty() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);
        ConsumerRecordsPublisher<Integer, Integer> source = ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1));

        StepVerifier.create(
                Flux.from(source)
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
        Publisher<ConsumerRecords<Integer, Integer>> source = ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1))
                .with(FilteringProcessor.create(ConsumerRecords::isEmpty));

        StepVerifier.create(
                Flux.from(source)
                        .timeout(Duration.ofMillis(200))
                        .map(ConsumerRecordsUtil::toList))
                .expectSubscription()
                .expectNext(records)
                .verifyError(TimeoutException.class);
    }
}
