package az.kon.academy.broker.consumer.config;

import az.kon.academy.broker.config.data.KafkaConfigData;
import az.kon.academy.broker.config.data.KafkaConsumerConfigData;
import az.kon.academy.broker.config.data.topic.KafkaConsumerTopicConfig;
import az.kon.academy.broker.config.data.topic.KafkaTopicsConfigData;
import az.kon.academy.broker.consumer.exception.KafkaConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Configuration
public class KafkaConsumerConfig<K extends Serializable, V extends Serializable> implements BeanFactoryPostProcessor, EnvironmentAware {
    private Environment environment;
    private KafkaConfigData kafkaConfigData;
    private KafkaConsumerConfigData kafkaConsumerConfigData;
    private KafkaTopicsConfigData kafkaTopicsConfigData;
    private KafkaConsumerTopicConfig kafkaDefaultConsumerTopicConfig;

    private Map<String, Object> consumerConfigs(KafkaConsumerTopicConfig topicConfig) {
        log.debug("Creating consumer configuration for topic config with group-id: {}",
                Objects.requireNonNullElse(topicConfig.getGroupId(), this.kafkaDefaultConsumerTopicConfig.getGroupId()));

        final var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigData.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaConsumerConfigData.getAutoOffsetReset());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.kafkaConsumerConfigData.getSessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, this.kafkaConsumerConfigData.getHeartbeatIntervalMs());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, this.kafkaConsumerConfigData.getMaxPollIntervalMs());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, this.kafkaConsumerConfigData.getAllowAutoCreateTopics());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, this.kafkaConsumerConfigData.getMaxPartitionFetchBytesDefault() * this.kafkaConsumerConfigData.getMaxPartitionFetchBytesBoostFactor());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.kafkaConsumerConfigData.getMaxPollRecords());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Objects.requireNonNullElse(topicConfig.getKeyDeserializer(), this.kafkaDefaultConsumerTopicConfig.getKeyDeserializer()));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Objects.requireNonNullElse(topicConfig.getValueDeserializer(), this.kafkaDefaultConsumerTopicConfig.getValueDeserializer()));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Objects.requireNonNullElse(topicConfig.getEnableAutoCommit(), this.kafkaDefaultConsumerTopicConfig.getEnableAutoCommit()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNullElse(topicConfig.getGroupId(), this.kafkaDefaultConsumerTopicConfig.getGroupId()));

        Map<String, String> defaultAdditionalProps = Objects.requireNonNullElse(this.kafkaDefaultConsumerTopicConfig.getAdditionalProps(), new HashMap<>());
        Map<String, String> topicAdditionalProps = Objects.requireNonNullElse(topicConfig.getAdditionalProps(), new HashMap<>());

        if( Objects.nonNull(this.kafkaConfigData.getSchemaRegistryUrlKey()) &&
                Objects.nonNull(this.kafkaConfigData.getSchemaRegistryUrl())) {
            props.put(kafkaConfigData.getSchemaRegistryUrlKey(), this.kafkaConfigData.getSchemaRegistryUrl());
            log.debug("Added schema registry configuration: {}={}",
                    this.kafkaConfigData.getSchemaRegistryUrlKey(), this.kafkaConfigData.getSchemaRegistryUrl());
        }

        if (!defaultAdditionalProps.isEmpty() || !topicAdditionalProps.isEmpty()) {
            Map<String, String> mergedAdditionalProps = new HashMap<>(defaultAdditionalProps);
            mergedAdditionalProps.putAll(topicAdditionalProps);
            props.putAll(mergedAdditionalProps);
            log.debug("Merged additional properties: {} default props, {} topic-specific props",
                    defaultAdditionalProps.size(), topicAdditionalProps.size());
        }

        log.debug("Consumer configuration created with {} properties: {}", props.size(), props);
        return props;
    }

    private KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> kafkaListenerContainerFactory(
            KafkaConsumerTopicConfig topicConfig
    ) {
        log.debug("Creating Kafka listener container factory with concurrency: {}, batch listener: {}",
                kafkaConsumerConfigData.getConcurrencyLevel(), kafkaConsumerConfigData.getBatchListener());

        ConcurrentKafkaListenerContainerFactory<K, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(this.consumerConfigs(topicConfig)));
        factory.setBatchListener(kafkaConsumerConfigData.getBatchListener());
        factory.setConcurrency(kafkaConsumerConfigData.getConcurrencyLevel());
        factory.setAutoStartup(kafkaConsumerConfigData.getAutoStartup());
        factory.getContainerProperties().setPollTimeout(kafkaConsumerConfigData.getPollTimeoutMs());
        if(topicConfig.getEnableAutoCommit() == false){
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        }
        log.debug("Kafka listener container factory created successfully");
        return factory;
    }

    public void registerFactoryBeans(ConfigurableListableBeanFactory beanFactory) {
        log.info("Starting to register consumer factory beans for {} topics", kafkaTopicsConfigData.getTopicConfigs().size());

        kafkaTopicsConfigData.getTopicConfigs().forEach((topicName, topicConfig) -> {
            String consumerFactory = this.kafkaTopicsConfigData.toConsumerFactory(topicName);
            try {
                log.debug("Registering consumer factory bean '{}' for topic '{}'", consumerFactory, topicName);

                KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> factory =
                        kafkaListenerContainerFactory(topicConfig.getConsumer());

                beanFactory.registerSingleton(consumerFactory, factory);

                log.info("Successfully registered consumer factory bean '{}' for topic '{}'", consumerFactory, topicName);

            } catch (Exception e) {
                log.error("Failed to register consumer factory bean '{}' for topic '{}': {}",
                        consumerFactory, topicName, e.getMessage(), e);
                throw new KafkaConsumerException("Failed to register kafka container factory: " + consumerFactory, e);
            }
        });
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        Binder binder = Binder.get(environment);
        this.kafkaConfigData = binder.bind("kafka.common", KafkaConfigData.class).orElse( new KafkaConfigData());
        this.kafkaConsumerConfigData = binder.bind("kafka.consumer", KafkaConsumerConfigData.class).orElse(new KafkaConsumerConfigData());;
        this.kafkaTopicsConfigData = binder.bind("kafka.topics", KafkaTopicsConfigData.class).orElse(new KafkaTopicsConfigData());
        this.kafkaDefaultConsumerTopicConfig = binder.bind("kafka.topic.default.consumer", KafkaConsumerTopicConfig.class).orElse(new KafkaConsumerTopicConfig());
        this.registerFactoryBeans(beanFactory);
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}