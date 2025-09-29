package az.kon.academy.broker.example.controller;

import az.kon.academy.broker.example.model.OrderEvent;
import az.kon.academy.broker.example.model.UserEvent;
import az.kon.academy.broker.example.model.avro.NotificationEvent;
import az.kon.academy.broker.example.service.EventService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;
    private final ApplicationContext applicationContext;

    @PostMapping("/user")
    public ResponseEntity<String> sendUserEvent() {
        var event = UserEvent.builder()
                .setUserId(UUID.randomUUID().toString())
                .setNotificationId(UUID.randomUUID().toString())
                .setType("login")
                .setMessage("message")
                .setTimestamp(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .build();
        eventService.sendUserEvent(event);
        return ResponseEntity.ok("User event sent successfully");
    }

    @PostMapping("/order")
    public ResponseEntity<String> sendOrderEvent(@RequestBody OrderEvent orderEvent) {
        eventService.sendOrderEvent(orderEvent);
        return ResponseEntity.ok("Order event sent successfully");
    }

    @PostMapping("/notification")
    public ResponseEntity<String> sendNotificationEvent() {
        var event = NotificationEvent.newBuilder()
                .setUserId(UUID.randomUUID().toString())
                .setNotificationId(UUID.randomUUID().toString())
                .setType("sms")
                .setMessage("message")
                .setTimestamp(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .build();
        eventService.sendNotificationEvent(event);
        return ResponseEntity.ok("Notification event sent successfully");
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Service is running");
    }
}