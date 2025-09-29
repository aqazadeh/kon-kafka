package az.kon.academy.broker.example.consumer;

import az.kon.academy.broker.consumer.KafkaConsumer;
import az.kon.academy.broker.example.model.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
@RequiredArgsConstructor
public class NotificationEventConsumer implements KafkaConsumer<String, NotificationEvent> {

    @Override
//    @KafkaListener(
//            topics = "user-events",
//            groupId = "user-events-group",
//            containerFactory = "userEventsContainerFactory"
//    )
    public void receive(ConsumerRecord<String, NotificationEvent> record) {
        try {
            log.info("Received notification event with key: {} from partition: {} with offset: {}",
                    record.key(), record.partition(), record.offset());

            NotificationEvent notificationEvent = record.value();

            log.info("Processing notification event: id={}, userId={}, type={}, message={}",
                    notificationEvent.getNotificationId(), notificationEvent.getUserId(),
                    notificationEvent.getType(), notificationEvent.getMessage());

            processNotificationEvent(notificationEvent);

        } catch (Exception e) {
            log.error("Error processing notification event: {}", record.value(), e);
        }
    }

    private void processNotificationEvent(NotificationEvent notificationEvent) {
        switch (notificationEvent.getType()) {
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