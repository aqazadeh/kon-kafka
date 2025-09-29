package az.kon.academy.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

public interface KafkaConsumer<K extends Serializable, V extends Serializable> {
    void receive(ConsumerRecord<K, V> record);
}
