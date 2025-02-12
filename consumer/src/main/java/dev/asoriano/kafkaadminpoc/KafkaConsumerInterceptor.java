package dev.asoriano.kafkaadminpoc;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;
import java.util.logging.Logger;

public class KafkaConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  private static final Logger logger = Logger.getLogger(KafkaConsumerInterceptor.class.getName());

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
    logger.info("Intercepted records: " + records);
    return records;
  }

  @Override
  public void onCommit(Map offsets) {
    logger.info("Intercepted commit: " + offsets);
  }

  @Override
  public void close() {
    logger.info("Closing interceptor");
  }

  @Override
  public void configure(Map<String, ?> configs) {
    logger.info("Configuring interceptor: " + configs);
  }
}
