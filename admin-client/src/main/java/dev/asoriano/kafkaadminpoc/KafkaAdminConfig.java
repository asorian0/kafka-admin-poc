package dev.asoriano.kafkaadminpoc;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

  @Bean
  public AdminClient kafkaAdminClient() {
    final Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", "localhost:29092");

    return KafkaAdminClient.create(configs);
  }
}
