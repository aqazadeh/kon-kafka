package az.kon.academy.broker.config.data.topic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.topics")
public class KafkaTopicsConfigData {
    private Integer numOfPartitions = 2;
    private Short replicationFactor = 1;
    private Map<String, KafkaTopicConfigData> topicConfigs = new HashMap<>();

    private String kebabToCamel(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }

        StringBuilder result = new StringBuilder();
        boolean toUpper = false;

        for (char c : value.toCharArray()) {
            if (c == '-') {
                toUpper = true;
            } else {
                if (toUpper) {
                    result.append(Character.toUpperCase(c));
                    toUpper = false;
                } else {
                    result.append(c);
                }
            }
        }

        return result.toString();
    }

    public String toProducerFactory(String topicName) {
        String name = this.kebabToCamel(topicName);
        return name + "ProducerFactory";
    }

    public String toKafkaTemplate(String topicName) {
        String name = this.kebabToCamel(topicName);
        return name + "KafkaTemplate";
    }

    public String toConsumerFactory(String topicName) {
        String name = this.kebabToCamel(topicName);
        return name + "ConsumerFactory";
    }
}
