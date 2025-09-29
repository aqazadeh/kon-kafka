package az.kon.academy.broker.producer;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;

public interface KafkaAvroProducer<K extends Serializable, V extends SpecificRecordBase> extends KafkaProducer<K, V> {
}
