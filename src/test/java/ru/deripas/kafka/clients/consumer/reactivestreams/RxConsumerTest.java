package ru.deripas.kafka.clients.consumer.reactivestreams;

import io.reactivex.Flowable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.deripas.kafka.clients.consumer.ConsumerRecordsUtil;
import ru.deripas.kafka.clients.consumer.SimpleMockConsumer;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;
import ru.deripas.reactivestreams.FilteringProcessor;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;

@Slf4j
public class RxConsumerTest {

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
    public void test() {
        List<ConsumerRecord<Integer, Integer>> records = mockConsumer.generateRecords(10, RandomUtils::nextInt, RandomUtils::nextInt);

        Flowable.fromPublisher(ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1)))
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
        ConsumerRecordsPublisher<Integer, Integer> source = ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1));

        Flowable.fromPublisher(source)
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
        Publisher<ConsumerRecords<Integer, Integer>> source = ConsumerRecordsPublisher.create(asyncConsumer, Duration.ofSeconds(1))
                .with(FilteringProcessor.create(ConsumerRecords::isEmpty));

        Flowable.fromPublisher(source)
                .timeout(200, TimeUnit.MILLISECONDS)
                .map(ConsumerRecordsUtil::toList)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValues(records)
                .assertError(TimeoutException.class);
    }
}
