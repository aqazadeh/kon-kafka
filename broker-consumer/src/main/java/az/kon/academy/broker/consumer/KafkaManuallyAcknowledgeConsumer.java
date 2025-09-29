package az.kon.academy.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Serializable;

public interface KafkaManuallyAcknowledgeConsumer<K extends Serializable, V extends Serializable> {
    void receive(ConsumerRecord<K, V> record, Acknowledgment acknowledgment);
}
