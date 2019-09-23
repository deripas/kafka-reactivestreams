package com.github.dao.kafka.clients.reactivestreams.consumer;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

@UtilityClass
public class ConsumerRecordsUtil {

    public static <K, V> List<ConsumerRecord<K, V>> toList(ConsumerRecords<K, V> records) {
        return stream(records.spliterator(), false).collect(Collectors.toList());
    }
}
