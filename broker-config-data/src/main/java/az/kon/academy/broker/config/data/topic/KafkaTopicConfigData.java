package az.kon.academy.broker.config.data.topic;

import lombok.Data;

@Data
public class KafkaTopicConfigData {
    private Integer numOfPartitions = 3;
    private Short replicationFactor = 2;
    private KafkaConsumerTopicConfig consumer;
    private KafkaProducerTopicConfig producer;
}
