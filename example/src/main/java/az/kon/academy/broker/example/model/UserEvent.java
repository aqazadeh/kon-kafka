package az.kon.academy.broker.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserEvent implements Serializable {
    private String userId;
    private String action;
    private String productId;
    private LocalDateTime timestamp;
}