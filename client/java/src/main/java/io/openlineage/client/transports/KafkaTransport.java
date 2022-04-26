package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.Utils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public final class KafkaTransport extends Transport {
  private final String topicName;
  private final String localServerId;
  private final KafkaProducer<String, String> producer;

  public KafkaTransport(@NonNull final KafkaConfig kafkaConfig) {
    this(new KafkaProducer<>(kafkaConfig.getProperties()), kafkaConfig);
  }

  public KafkaTransport(
      @NonNull final KafkaProducer<String, String> kafkaProducer,
      @NonNull final KafkaConfig kafkaConfig) {
    super(Type.KAFKA);
    this.topicName = kafkaConfig.getTopicName();
    this.localServerId = kafkaConfig.getLocalServerId();
    this.producer = kafkaProducer;
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = Utils.toJson(runEvent);
    log.debug("Received lineage event: {}", eventAsJson);
    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topicName, localServerId, eventAsJson);
    try {
      producer.send(record);
    } catch (Exception e) {
      log.error("Failed to collect lineage event: {}", eventAsJson, e);
    }
  }
}
