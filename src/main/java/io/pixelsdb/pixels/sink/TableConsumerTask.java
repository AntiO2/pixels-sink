package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.writer.CsvWriter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TableConsumerTask implements Runnable {

    private PixelsSinkConfig pixelsSinkConfig;
    private Properties kafkaProperties;
    private String topic;

    public TableConsumerTask(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties, String topic) {
        this.pixelsSinkConfig = pixelsSinkConfig;
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    @Override
    public void run() {
        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            consumer.poll(1000).forEach(record -> {
                Map<String, Object> message = record.value();
                String tableName = extractTableName(record.topic());

                try {
                    CsvWriter.writeToCsv(tableName + ".csv", message, pixelsSinkConfig.getCaptureDatabase());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}
