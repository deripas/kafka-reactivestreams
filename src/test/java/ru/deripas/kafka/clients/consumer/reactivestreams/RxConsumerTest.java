package ru.deripas.kafka.clients.consumer.reactivestreams;

import io.reactivex.Flowable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.deripas.kafka.clients.consumer.SimpleMockConsumer;
import ru.deripas.kafka.clients.consumer.async.AsyncConsumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;

@Slf4j
public class RxConsumerTest {

    private SimpleMockConsumer<Integer, Integer> mockConsumer;
    private AsyncConsumer<Integer, Integer> asyncConsumer;

    @BeforeMethod
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

        Flowable.fromPublisher(ConsumerRecordsPublisher.create(asyncConsumer))
                .take(1)
                .flatMap(Flowable::fromIterable)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertSubscribed()
                .assertValueSequence(records)
                .assertNoErrors()
                .assertComplete();
    }
}
