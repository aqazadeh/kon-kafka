package az.kon.academy.broker.example.controller;

import az.kon.academy.broker.example.model.NotificationEvent;
import az.kon.academy.broker.example.model.OrderEvent;
import az.kon.academy.broker.example.model.UserEvent;
import az.kon.academy.broker.example.service.EventService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;
    private final ApplicationContext applicationContext;

    @PostMapping("/user")
    public ResponseEntity<String> sendUserEvent(@RequestBody UserEvent userEvent) {
        eventService.sendUserEvent(userEvent);
        return ResponseEntity.ok("User event sent successfully");
    }

    @PostMapping("/order")
    public ResponseEntity<String> sendOrderEvent(@RequestBody OrderEvent orderEvent) {
        eventService.sendOrderEvent(orderEvent);
        return ResponseEntity.ok("Order event sent successfully");
    }

    @PostMapping("/notification")
    public ResponseEntity<String> sendNotificationEvent(@RequestBody NotificationEvent notificationEvent) {
        eventService.sendNotificationEvent(notificationEvent);
        return ResponseEntity.ok("Notification event sent successfully");
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Service is running");
    }
}