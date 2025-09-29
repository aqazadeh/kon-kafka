package az.kon.academy.broker.example.producer;

import az.kon.academy.broker.example.model.UserEvent;
import az.kon.academy.broker.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserEventProducer {

    private final KafkaProducer<String, UserEvent> kafkaProducer;

    public UserEventProducer(KafkaProducer<String, UserEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void send(UserEvent userEvent) {
        kafkaProducer.send("user-events", userEvent.getUserId(), userEvent,
                (result, ex) -> {
                    if (ex == null) {
                        log.info("User event sent successfully: {}", result.getRecordMetadata());
                    } else {
                        log.info("Failed to send user event: {}", ex.getMessage());
                    }
                });
    }

}
