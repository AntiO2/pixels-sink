package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.TableConsumerTask;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;

public class TopicMonitor extends Thread {

    private Properties kafkaProperties;
    private PixelsSinkConfig pixelsSinkConfig;
    private String baseTopic;
    private String[] includeTables;
    private Set<String> subscribedTopics;
    private ExecutorService executorService;


    public TopicMonitor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties) {
        this.pixelsSinkConfig = pixelsSinkConfig;
        this.kafkaProperties = kafkaProperties;
        this.baseTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getCaptureDatabase();
        this.includeTables = pixelsSinkConfig.getIncludeTables();
        this.subscribedTopics = new HashSet<>();
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer(kafkaProperties);
        consumer.subscribe(Collections.singletonList(baseTopic)); // 订阅 baseTopic

        while (true) {
            consumer.poll(1000).forEach(record -> {
                String topic = record.topic();
                String tableName = extractTableName(topic);
                if (!subscribedTopics.contains(topic) && shouldProcessTable(tableName)) {
                    TableConsumerTask task = new TableConsumerTask(pixelsSinkConfig, kafkaProperties, topic);
                    executorService.submit(task);
                    subscribedTopics.add(topic);
                }
            });
        }
    }

    private boolean shouldProcessTable(String tableName) {
        if (includeTables.length == 0) {
            return true;
        }
        for (String table : includeTables) {
            if (tableName.equals(table)) {
                return true;
            }
        }
        return false;
    }

    private String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}