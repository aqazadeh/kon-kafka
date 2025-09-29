package az.kon.academy.broker.example.producer;

import az.kon.academy.broker.example.model.avro.NotificationEvent;
import az.kon.academy.broker.producer.KafkaProducer;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventProducer {

    private final KafkaProducer<String, NotificationEvent> kafkaProducer;

    public NotificationEventProducer(KafkaProducer<String, NotificationEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void send(NotificationEvent notificationEvent) {
        kafkaProducer.send("notification-events", notificationEvent.getNotificationId().toString(), notificationEvent,
                (result, ex) -> {
                    if (ex == null) {
                        System.out.println("Notification event sent successfully: " + result.getRecordMetadata());
                    } else {
                        System.err.println("Failed to send notification event: " + ex.getMessage());
                    }
                });
    }
}
