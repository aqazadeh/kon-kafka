package az.kon.academy.broker.producer;

import az.kon.academy.broker.config.data.topic.KafkaTopicsConfigData;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Slf4j
@Component
public class SimpleKafkaAvroProducer<K extends Serializable, V extends SpecificRecordBase>
        extends SimpleKafkaProducer<K, V>
        implements KafkaAvroProducer<K, V> {

    public SimpleKafkaAvroProducer(KafkaTopicsConfigData topicsConfigData, ApplicationContext applicationContext) {
        super(topicsConfigData, applicationContext);
    }

}
