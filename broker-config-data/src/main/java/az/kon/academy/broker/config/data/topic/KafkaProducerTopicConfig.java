package az.kon.academy.broker.config.data.topic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties("kafka.topic.default.producer")
public class KafkaProducerTopicConfig {
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String acks = "all";
    private Map<String, String> additionalProps = new HashMap<>();

}
