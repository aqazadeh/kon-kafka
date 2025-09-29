package az.kon.academy.broker.config.data.topic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "kafka.topic.default.consumer")
public class KafkaConsumerTopicConfig {
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private Boolean enableAutoCommit = true;
    private String groupId;
    private Map<String, String> additionalProps = new HashMap<>();
}
