package az.kon.academy.broker.example.producer;

import az.kon.academy.broker.example.model.OrderEvent;
import az.kon.academy.broker.producer.KafkaProducer;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {
    private final KafkaProducer<String, OrderEvent> kafkaProducer;

    public OrderEventProducer(KafkaProducer<String, OrderEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void send(OrderEvent orderEvent) {
        kafkaProducer.send("order-events", orderEvent.getOrderId(), orderEvent,
                (result, ex) -> {
                    if (ex == null) {
                        System.out.println("User event sent successfully: " + result.getRecordMetadata().partition());
                    } else {
                        System.err.println("Failed to send user event: " + ex.getMessage());
                    }
                });
    }
}
