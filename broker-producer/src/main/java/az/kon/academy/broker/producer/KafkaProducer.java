package az.kon.academy.broker.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.support.SendResult;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface KafkaProducer<K extends Serializable, V extends Serializable> {

    void send(ProducerRecord<K, V> record, BiConsumer<SendResult<K, V>, Throwable> callback);

    default void send(ProducerRecord<K, V> record) {
        send(record, (result, ex) -> {
        });
    }

    default void send(String topic, V value) {
        send(new ProducerRecord<>(topic, value));
    }

    default void send(String topic, V value, BiConsumer<SendResult<K, V>, Throwable> callback) {
        send(new ProducerRecord<>(topic, value), callback);
    }

    default void send(String topic, K key, V value) {
        send(new ProducerRecord<>(topic, key, value));
    }

    default void send(String topic, K key, V value, BiConsumer<SendResult<K, V>, Throwable> callback) {
        send(new ProducerRecord<>(topic, key, value), callback);
    }

    default void send(String topic, Integer partition, K key, V value) {
        send(new ProducerRecord<>(topic, partition, key, value));
    }

    default void send(String topic, Integer partition, K key, V value, BiConsumer<SendResult<K, V>, Throwable> callback) {
        send(new ProducerRecord<>(topic, partition, key, value), callback);
    }

    default void send(String topic, Integer partition, Long timestamp, K key, V value) {
        send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    default void send(String topic, Integer partition, Long timestamp, K key, V value,
                      BiConsumer<SendResult<K, V>, Throwable> callback) {
        send(new ProducerRecord<>(topic, partition, timestamp, key, value), callback);
    }

    default void send(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        send(new ProducerRecord<>(topic, partition, timestamp, key, value, headers));
    }

    default void send(String topic, Integer partition, Long timestamp, K key, V value,
                      Iterable<Header> headers, BiConsumer<SendResult<K, V>, Throwable> callback) {
        send(new ProducerRecord<>(topic, partition, timestamp, key, value, headers), callback);
    }
}
