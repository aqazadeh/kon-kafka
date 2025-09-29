package az.kon.academy.broker.config.data;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerConfigData {
    private String autoOffsetReset = "earliest";
    private Boolean batchListener = false;
    private Boolean autoStartup = true;
    private Boolean allowAutoCreateTopics = true;
    private Integer sessionTimeoutMs = 10000;
    private Integer heartbeatIntervalMs = 3000;
    private Integer concurrencyLevel = 1;
    private Integer maxPollIntervalMs = 300000;
    private Long pollTimeoutMs = 1000L;
    private Integer maxPollRecords = 500;
    private Integer maxPartitionFetchBytesDefault = 1048576;
    private Integer maxPartitionFetchBytesBoostFactor = 1;
}
