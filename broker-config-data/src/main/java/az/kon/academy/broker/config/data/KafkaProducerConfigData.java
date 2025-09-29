package az.kon.academy.broker.config.data;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaProducerConfigData {
    private String compressionType = "snappy";
    private Integer batchSize = 16384;
    private Integer batchSizeBoostFactor = 1;
    private Integer lingerMs = 1;
    private Integer requestTimeoutMs = 30000;
    private Integer retryCount = 3;
}