package dev.asoriano.kafkaadminpoc;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(id = "test-group", topics = "test-topic")
    public void listen(String message) {
        System.out.println("Received message: " + message);
    }
}
