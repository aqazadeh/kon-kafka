package az.kon.academy.broker.producer.config;

import az.kon.academy.broker.config.data.KafkaConfigData;
import az.kon.academy.broker.config.data.KafkaConsumerConfigData;
import az.kon.academy.broker.config.data.KafkaProducerConfigData;
import az.kon.academy.broker.config.data.topic.KafkaProducerTopicConfig;
import az.kon.academy.broker.config.data.topic.KafkaTopicsConfigData;
import az.kon.academy.broker.producer.exception.KafkaProducerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Configuration
public class KafkaProducerConfig<K extends Serializable, V extends SpecificRecordBase> implements BeanFactoryPostProcessor, EnvironmentAware {

    private Environment environment;
    private KafkaConfigData kafkaConfigData;
    private KafkaProducerConfigData kafkaProducerConfigData;
    private KafkaTopicsConfigData kafkaTopicsConfigData;
    private KafkaProducerTopicConfig kafkaDefaultProducerTopicConfig;

    public Map<String, Object> producerConfig(KafkaProducerTopicConfig topicConfig) {
        log.debug("Creating producer configuration with batch size: {}, compression: {}",
                kafkaProducerConfigData.getBatchSize() * kafkaProducerConfigData.getBatchSizeBoostFactor(),
                kafkaProducerConfigData.getCompressionType());

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigData.getBootstrapServers());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, this.kafkaProducerConfigData.getBatchSize() * this.kafkaProducerConfigData.getBatchSizeBoostFactor());
        props.put(ProducerConfig.LINGER_MS_CONFIG, this.kafkaProducerConfigData.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.kafkaProducerConfigData.getCompressionType());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.kafkaProducerConfigData.getRequestTimeoutMs());
        props.put(ProducerConfig.RETRIES_CONFIG, this.kafkaProducerConfigData.getRetryCount());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Objects.requireNonNullElse(topicConfig.getKeySerializer(), this.kafkaDefaultProducerTopicConfig.getKeySerializer()));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Objects.requireNonNullElse(topicConfig.getValueSerializer(), this.kafkaDefaultProducerTopicConfig.getValueSerializer()));
        props.put(ProducerConfig.ACKS_CONFIG, Objects.requireNonNullElse(topicConfig.getAcks(), this.kafkaDefaultProducerTopicConfig.getAcks()));

        if (Objects.nonNull(this.kafkaConfigData.getSchemaRegistryUrlKey()) &&
                Objects.nonNull(this.kafkaConfigData.getSchemaRegistryUrl())) {
            props.put(kafkaConfigData.getSchemaRegistryUrlKey(), this.kafkaConfigData.getSchemaRegistryUrl());
            log.debug("Added schema registry configuration: {}={}",
                    this.kafkaConfigData.getSchemaRegistryUrlKey(), this.kafkaConfigData.getSchemaRegistryUrl());
        }

        Map<String, String> defaultAdditionalProps = Objects.requireNonNullElse(this.kafkaDefaultProducerTopicConfig.getAdditionalProps(), new HashMap<>());
        Map<String, String> topicAdditionalProps = Objects.requireNonNullElse(topicConfig.getAdditionalProps(), new HashMap<>());
        if (!defaultAdditionalProps.isEmpty() || !topicAdditionalProps.isEmpty()) {
            Map<String, String> mergedAdditionalProps = new HashMap<>(defaultAdditionalProps);
            mergedAdditionalProps.putAll(topicAdditionalProps);
            props.putAll(mergedAdditionalProps);
            log.debug("Merged additional properties: {} default props, {} topic-specific props",
                    defaultAdditionalProps.size(), topicAdditionalProps.size());
        }
        log.debug("Producer configuration created with {} properties: {}", props.size(), props);
        return props;
    }

    public ProducerFactory<K, V> producerFactory(KafkaProducerTopicConfig topicConfig) {
        log.debug("Creating producer factory with serializers - Key: {}, Value: {}",
                Objects.requireNonNullElse(topicConfig.getKeySerializer(), this.kafkaDefaultProducerTopicConfig.getKeySerializer()),
                Objects.requireNonNullElse(topicConfig.getValueSerializer(), this.kafkaDefaultProducerTopicConfig.getValueSerializer()));
        return new DefaultKafkaProducerFactory<>(this.producerConfig(topicConfig));
    }

    public KafkaTemplate<K, V> kafkaTemplate(KafkaProducerTopicConfig topicConfig) {
        log.debug("Creating Kafka template with acks: {}",
                Objects.requireNonNullElse(topicConfig.getAcks(), this.kafkaDefaultProducerTopicConfig.getAcks()));
        return new KafkaTemplate<>(this.producerFactory(topicConfig));
    }

    public void registerProducerBeans(ConfigurableListableBeanFactory beanFactory) {
        log.info("Starting to register producer beans for {} topics",
                kafkaTopicsConfigData.getTopicConfigs().size());

        kafkaTopicsConfigData.getTopicConfigs().forEach((topicName, topicConfig) -> {
            log.debug("Registering producer beans for topic '{}'", topicName);

            var producerFactory = this.kafkaTopicsConfigData.toProducerFactory(topicName);
            log.debug("Processing producer factory bean name '{}'", producerFactory);

                try {
                    log.debug("Registering producer factory bean '{}' for topic '{}'", producerFactory, topicName);
                    ProducerFactory<K, V> factory = producerFactory(
                            topicConfig.getProducer()
                    );
                    beanFactory.registerSingleton(producerFactory, factory);
                    log.info("Successfully registered producer factory bean '{}' for topic '{}'", producerFactory, topicName);
                } catch (Exception e) {
                    log.error("Failed to register producer factory '{}' for topic '{}': {}",
                            producerFactory, topicName, e.getMessage(), e);
                    throw new KafkaProducerException("Failed to register kafka producer factory: " + producerFactory, e);
                }

            String kafkaTemplate = this.kafkaTopicsConfigData.toKafkaTemplate(topicName);
            try {
                log.debug("Registering kafka template bean '{}' for topic '{}'", kafkaTemplate, topicName);
                KafkaTemplate<K, V> template = kafkaTemplate(
                        topicConfig.getProducer()
                );
                beanFactory.registerSingleton(kafkaTemplate, template);
                log.info("Successfully registered kafka template bean '{}' for topic '{}'", kafkaTemplate, topicName);
            } catch (Exception e) {
                log.error("Failed to register kafka template '{}' for topic '{}': {}",
                        kafkaTemplate, topicName, e.getMessage(), e);
                throw new KafkaProducerException("Failed to register kafka template: " + kafkaTemplate, e);
            }
        });
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        log.info("Starting Kafka producer factory bean registration");
        Binder binder = Binder.get(environment);
        this.kafkaConfigData = binder.bind("kafka.common", KafkaConfigData.class).orElse(new KafkaConfigData());
        this.kafkaProducerConfigData = binder.bind("kafka.producer", KafkaProducerConfigData.class).orElse(new KafkaProducerConfigData());
        this.kafkaTopicsConfigData = binder.bind("kafka.topics", KafkaTopicsConfigData.class).orElse(new KafkaTopicsConfigData());
        this.kafkaDefaultProducerTopicConfig = binder.bind("kafka.topic.default.producer", KafkaProducerTopicConfig.class).orElse(new KafkaProducerTopicConfig());
        this.registerProducerBeans(beanFactory);
        log.info("Completed Kafka producer factory bean registration");
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
