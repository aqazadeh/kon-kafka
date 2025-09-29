package az.kon.academy.broker.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent implements Serializable {
    private String orderId;
    private String userId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime timestamp;
}