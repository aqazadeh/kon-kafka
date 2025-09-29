package az.kon.academy.broker.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent implements Serializable {
    private String notificationId;
    private String userId;
    private String type;
    private String message;
    private LocalDateTime timestamp;
}