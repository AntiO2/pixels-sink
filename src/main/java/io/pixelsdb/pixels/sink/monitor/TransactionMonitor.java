package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.core.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.core.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionMonitor implements Runnable, StoppableMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMonitor.class);

    private final String transactionTopic;
    private final KafkaConsumer<String, TransactionMetadataValue.TransactionMetadata> consumer;
    private final TransactionCoordinator transactionCoordinator;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public TransactionMonitor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties) {
        this.transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getTransactionTopicSuffix();
        this.consumer = new KafkaConsumer<>(kafkaProperties);
        this.transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(transactionTopic));
            LOGGER.info("Started transaction monitor for topic: {}", transactionTopic);

            while (running.get()) {
                try {
                    ConsumerRecords<String, TransactionMetadataValue.TransactionMetadata> records =
                            consumer.poll(Duration.ofMillis(500));

                    for (ConsumerRecord<String, TransactionMetadataValue.TransactionMetadata> record : records) {
                        TransactionMetadataValue.TransactionMetadata transaction = record.value();
                        LOGGER.debug("Processing transaction event: {}", transaction.getId());
                        transactionCoordinator.processTransactionEvent(transaction);
                    }
                } catch (WakeupException e) {
                    if (running.get()) {
                        LOGGER.warn("Consumer wakeup unexpectedly", e);
                    }
                }
            }
        } finally {
            closeResources();
            LOGGER.info("Transaction monitor stopped");
        }
    }

    @Override
    public void stopMonitor() {
        LOGGER.info("Stopping transaction monitor");
        running.set(false);
        consumer.wakeup();
    }

    private void closeResources() {
        try {
            if (consumer != null) {
                consumer.close(Duration.ofSeconds(5));
                LOGGER.debug("Kafka consumer closed");
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Kafka consumer", e);
        }
    }
}
