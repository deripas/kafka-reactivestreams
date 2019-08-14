package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.deripas.reactivestreams.DelegateProcessor;

@Slf4j
@NoArgsConstructor(staticName = "create")
public class IgnoreEmptyConsumerRecordsProcessor<K, V> extends DelegateProcessor<ConsumerRecords<K, V>> {

    @Override
    public void onNext(ConsumerRecords<K, V> consumerRecords) {
        if (consumerRecords.isEmpty()) {
            log.info("empty records, try again");
            subscription().request(1);
        } else {
            super.onNext(consumerRecords);
        }
    }
}
