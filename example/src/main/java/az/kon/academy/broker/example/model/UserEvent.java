package az.kon.academy.broker.example.model;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder(setterPrefix="set")
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserEvent implements Serializable {
    private String notificationId;
    private String userId;
    private String type;
    private String message;
    private Long timestamp;
}