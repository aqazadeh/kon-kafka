package az.kon.academy.broker.example.consumer;

import az.kon.academy.broker.consumer.KafkaConsumer;
import az.kon.academy.broker.example.model.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
//KafkaManuallyAcknowledgeConsumer
public class UserEventConsumer implements KafkaConsumer<String, UserEvent> {

    @Override
    @KafkaListener(
            topics = "user-events",
            containerFactory = "userEventsConsumerFactory"
    )
    public void receive(ConsumerRecord<String, UserEvent> record) {
        try {
            log.info("Received user event with key: {} from partition: {} with offset: {}",
                    record.key(), record.partition(), record.offset());

            UserEvent userEvent = record.value();

            log.info("Processing user event: userId={}, action={}, timestamp={}",
                    userEvent.getUserId(), userEvent.getType(), userEvent.getTimestamp());

            processUserEvent(userEvent);

        } catch (Exception e) {
            log.error("Error processing user event: {}", record.value(), e);
        }
//        finally {
//            acknowledgment.acknowledge();
//        }
    }

    private void processUserEvent(UserEvent userEvent) {
        switch (userEvent.getType()) {
            case "login":
                log.info("User {} logged in at {}", userEvent.getUserId(), userEvent.getTimestamp());
                break;
            case "logout":
                log.info("User {} logged out at {}", userEvent.getUserId(), userEvent.getTimestamp());
                break;
            case "register":
                log.info("User {} registered at {}", userEvent.getUserId(), userEvent.getTimestamp());
                break;
            case "view_product":
                log.info("User {} viewed product {} at {}",
                        userEvent.getUserId(), userEvent.getNotificationId(), userEvent.getTimestamp());
                break;
            default:
                log.info("Unknown user action: {} for user {}", userEvent.getType(), userEvent.getUserId());
        }
    }
}