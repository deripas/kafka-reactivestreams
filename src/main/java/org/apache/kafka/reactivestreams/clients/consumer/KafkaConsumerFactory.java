package org.apache.kafka.reactivestreams.clients.consumer;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@AllArgsConstructor(staticName = "create")
public class KafkaConsumerFactory<K, V> implements AsyncConsumerFactory<K, V> {

    @Override
    public Consumer<K, V> apply(ConsumerConfig<K, V> config) {
        return new KafkaConsumer<K, V>(config.getProperties(), config.getKeyDeserializer(), config.getValueDeserializer());
    }
}
