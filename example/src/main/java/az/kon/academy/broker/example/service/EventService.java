package az.kon.academy.broker.example.service;

import az.kon.academy.broker.example.model.NotificationEvent;
import az.kon.academy.broker.example.model.OrderEvent;
import az.kon.academy.broker.example.model.UserEvent;
import az.kon.academy.broker.example.producer.NotificationEventProducer;
import az.kon.academy.broker.example.producer.OrderEventProducer;
import az.kon.academy.broker.example.producer.UserEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final UserEventProducer userEventProducer;
    private final NotificationEventProducer notificationEventProducer;
    private final OrderEventProducer orderEventProducer;

    public void sendUserEvent(UserEvent userEvent) {
        userEventProducer.send(userEvent);
    }

    public void sendOrderEvent(OrderEvent orderEvent) {
       orderEventProducer.send(orderEvent);
    }

    public void sendNotificationEvent(NotificationEvent notificationEvent) {
        notificationEventProducer.send(notificationEvent);
    }
}