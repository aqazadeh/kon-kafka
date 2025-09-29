package az.kon.academy.broker.config.data;

import az.kon.academy.broker.config.data.topic.KafkaTopicsConfigData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@Configuration
public class KafkaTopicInitializer {
    private final KafkaAdmin kafkaAdmin;
    private final KafkaTopicsConfigData topicsConfigData;

    public KafkaTopicInitializer(KafkaAdmin kafkaAdmin, KafkaTopicsConfigData topicsConfigData) {
        this.kafkaAdmin = kafkaAdmin;
        this.topicsConfigData = topicsConfigData;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void createTopics() {
        List<NewTopic> topics = new ArrayList<>();
        topicsConfigData.getTopicConfigs().forEach((topicName, topicConfig) -> {
            NewTopic newTopic = TopicBuilder.name(topicName)
                    .partitions(Objects.requireNonNullElse(
                            topicConfig.getNumOfPartitions(),
                            topicsConfigData.getTopicConfigs().get(topicName).getNumOfPartitions()))
                    .replicas(Objects.requireNonNullElse(
                            topicConfig.getReplicationFactor(),
                            topicsConfigData.getTopicConfigs().get(topicName).getReplicationFactor()))
                    .build();
            topics.add(newTopic);
        });

        kafkaAdmin.createOrModifyTopics(topics.toArray(new NewTopic[0]));
    }
}
