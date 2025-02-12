package dev.asoriano.kafkaadminpoc;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {

  private static final Logger logger = Logger.getLogger(KafkaConsumerRebalanceListener.class.getName());

  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final Map<String, Boolean> pausedPartitions = new HashMap<>(Map.of("test-group", false));

  public KafkaConsumerRebalanceListener(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    logger.info("Partitions revoked: " + partitions);

    final MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("test-group");

    if (listenerContainer != null) {
      logger.info("Pausing test-group listener container");
      listenerContainer.pause();
      pausedPartitions.put("test-group", true);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    logger.info("Partitions assigned: " + partitions);

    final MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("test-group");

    if (listenerContainer != null && pausedPartitions.get("test-group")) {
      logger.info("Assuring partition is not being consumed by test-group");
      listenerContainer.pause();
    }
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    logger.info("Partitions lost: " + partitions);
  }
}
