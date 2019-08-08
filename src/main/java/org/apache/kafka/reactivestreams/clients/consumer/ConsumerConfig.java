package org.apache.kafka.reactivestreams.clients.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

@Getter
@AllArgsConstructor
public class ConsumerConfig<K, V> {
    private final Map<String, Object> properties;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public OffsetResetStrategy getOffsetResetStrategy() {
        String value = properties.getOrDefault(AUTO_OFFSET_RESET_CONFIG, "none")
                .toString()
                .toUpperCase();
        return OffsetResetStrategy.valueOf(value);
    }
}
