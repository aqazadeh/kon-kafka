package az.kon.academy.broker.example.consumer;

import az.kon.academy.broker.consumer.KafkaConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import az.kon.academy.broker.example.model.avro.NotificationEvent;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NotificationEventConsumer implements KafkaConsumer<String, NotificationEvent> {

    @Override
    @KafkaListener(
            topics = "notification-events",
            containerFactory = "notificationEventsConsumerFactory"
    )
    public void receive(ConsumerRecord<String, NotificationEvent> record) {
        try {
            log.info("Received notification event with key: {} from partition: {} with offset: {}",
                    record.key(), record.partition(), record.offset());
            System.out.println(record.value().getClass());
            NotificationEvent notificationEvent = record.value();

            log.info("Processing notification event: id={}, userId={}, type={}, message={}",
                    notificationEvent.getNotificationId(), notificationEvent.getNotificationId(),
                    notificationEvent.getType(), notificationEvent.getMessage());

            processNotificationEvent(notificationEvent);

        } catch (Exception e) {
            log.error("Error processing notification event: {}", record.value(), e);
        }
    }

    private void processNotificationEvent(NotificationEvent notificationEvent) {
        switch (String.valueOf(notificationEvent.getType())) {
            case "email":
                log.info("Sending email notification to user {}: {}",
                        notificationEvent.getUserId(), notificationEvent.getMessage());
                sendEmailNotification(notificationEvent);
                break;
            case "sms":
                log.info("Sending SMS notification to user {}: {}",
                        notificationEvent.getUserId(), notificationEvent.getMessage());
                sendSmsNotification(notificationEvent);
                break;
            case "push":
                log.info("Sending push notification to user {}: {}",
                        notificationEvent.getUserId(), notificationEvent.getMessage());
                sendPushNotification(notificationEvent);
                break;
            default:
                log.info("Unknown notification type: {} for user {}",
                        notificationEvent.getType(), notificationEvent.getUserId());
        }
    }

    private void sendEmailNotification(NotificationEvent notificationEvent) {
        log.info("Email sent to user {} with notification ID {}",
                notificationEvent.getUserId(), notificationEvent.getNotificationId());
    }

    private void sendSmsNotification(NotificationEvent notificationEvent) {
        log.info("SMS sent to user {} with notification ID {}",
                notificationEvent.getUserId(), notificationEvent.getNotificationId());
    }

    private void sendPushNotification(NotificationEvent notificationEvent) {
        log.info("Push notification sent to user {} with notification ID {}",
                notificationEvent.getUserId(), notificationEvent.getNotificationId());
    }
}