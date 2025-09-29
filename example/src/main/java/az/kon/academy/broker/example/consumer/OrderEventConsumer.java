package az.kon.academy.broker.example.consumer;

import az.kon.academy.broker.consumer.KafkaConsumer;
import az.kon.academy.broker.example.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
@RequiredArgsConstructor
public class OrderEventConsumer implements KafkaConsumer<String, OrderEvent> {

    @Override
    @KafkaListener(topics = "order-events", groupId = "order-events-group",
                  containerFactory = "orderEventsConsumerFactory")
    public void receive(ConsumerRecord<String, OrderEvent> record) {
        try {
            log.info("Received order event with key: {} from partition: {} with offset: {}",
                    record.key(), record.partition(), record.offset());

            OrderEvent orderEvent = record.value();

            log.info("Processing order event: orderId={}, userId={}, amount={}, status={}",
                    orderEvent.getOrderId(), orderEvent.getUserId(),
                    orderEvent.getAmount(), orderEvent.getStatus());

            processOrderEvent(orderEvent);

        } catch (Exception e) {
            log.error("Error processing order event: {}", record.value(), e);
        }
    }

    private void processOrderEvent(OrderEvent orderEvent) {
        switch (orderEvent.getStatus()) {
            case "created":
                log.info("Order {} created for user {} with amount {}",
                        orderEvent.getOrderId(), orderEvent.getUserId(), orderEvent.getAmount());
                break;
            case "paid":
                log.info("Order {} paid by user {} with amount {}",
                        orderEvent.getOrderId(), orderEvent.getUserId(), orderEvent.getAmount());
                break;
            case "shipped":
                log.info("Order {} shipped for user {}",
                        orderEvent.getOrderId(), orderEvent.getUserId());
                break;
            case "delivered":
                log.info("Order {} delivered to user {}",
                        orderEvent.getOrderId(), orderEvent.getUserId());
                break;
            case "cancelled":
                log.info("Order {} cancelled for user {}",
                        orderEvent.getOrderId(), orderEvent.getUserId());
                break;
            default:
                log.info("Unknown order status: {} for order {}", orderEvent.getStatus(), orderEvent.getOrderId());
        }
    }
}