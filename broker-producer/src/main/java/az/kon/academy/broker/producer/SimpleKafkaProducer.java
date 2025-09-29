package az.kon.academy.broker.producer;

import az.kon.academy.broker.config.data.topic.KafkaTopicsConfigData;
import az.kon.academy.broker.producer.exception.KafkaProducerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Slf4j
@Primary
@Component
public class SimpleKafkaProducer<K extends Serializable, V extends Serializable> implements KafkaProducer<K, V> {
    private final KafkaTopicsConfigData topicsConfigData;
    private final ApplicationContext applicationContext;

    public SimpleKafkaProducer(KafkaTopicsConfigData topicsConfigData,
                               ApplicationContext applicationContext) {
        this.topicsConfigData = topicsConfigData;
        this.applicationContext = applicationContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void send(ProducerRecord<K, V> record, BiConsumer<SendResult<K, V>, Throwable> callback) {
        log.info("Sending message={} to topic={}", record.value(), record.topic());
        var kafkaTemplateBeanName = topicsConfigData.toKafkaTemplate(record.topic());
        var kafkaTemplate = (KafkaTemplate<K, V>) applicationContext.getBean(kafkaTemplateBeanName);
        try {
            CompletableFuture<SendResult<K, V>> kafkaResultFuture = kafkaTemplate.send(record);
            kafkaResultFuture.whenComplete(callback);
        } catch (KafkaException e) {
            log.error("Error on kafka producer with key: {}, message: {} and exception: {}",
                    record.key(),
                    record.value(),
                    e.getMessage());
            throw new KafkaProducerException("Error on kafka producer with key: " + record.key() + " and message: " + record.value());
        }
    }
}
