/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.event.TablePipelineManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TopicProcessor extends TablePipelineManager implements StoppableProcessor, Runnable
{

    private static final Logger log = LoggerFactory.getLogger(TopicProcessor.class);
    private final Properties kafkaProperties;
    private final PixelsSinkConfig pixelsSinkConfig;
    private final String[] includeTables;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final String bootstrapServers;
    private final String baseTopic;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<String, TableMonitor> activeTasks = new ConcurrentHashMap<>(); // track row event consumer
    private AdminClient adminClient;
    private Timer timer;

    public TopicProcessor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties)
    {
        this.pixelsSinkConfig = pixelsSinkConfig;
        this.kafkaProperties = kafkaProperties;
        this.baseTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getCaptureDatabase();
        this.includeTables = pixelsSinkConfig.getIncludeTables();
        this.bootstrapServers = pixelsSinkConfig.getBootstrapServers();
    }

    private static Set<String> filterTopics(Set<String> topics, String prefix)
    {
        return topics.stream()
                .filter(t -> t.startsWith(prefix))
                .collect(Collectors.toSet());
    }

    @Override
    public void run()
    {
        try
        {
            initializeResources();
            startMonitoringCycle();
        } finally
        {
            cleanupResources();
            log.info("Topic monitor stopped");
        }
    }

    @Override
    public void stopProcessor()
    {
        log.info("Initiating topic monitor shutdown...");
        running.set(false);
        interruptMonitoring();
        shutdownConsumerTasks();
        awaitTermination();
    }

    private void shutdownConsumerTasks()
    {
        log.info("Shutting down {} active consumer tasks", activeTasks.size());
        activeTasks.forEach((topic, task) ->
        {
            log.info("Stopping consumer for topic: {}", topic);
            task.shutdown();
        });
        activeTasks.clear();
    }

    private void awaitTermination()
    {
        try
        {
            if (executorService != null && !executorService.awaitTermination(30, TimeUnit.SECONDS))
            {
                log.warn("Forcing shutdown of remaining tasks");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void initializeResources()
    {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        this.adminClient = AdminClient.create(props);
        this.timer = new Timer("TopicProcessor-Timer", true);
        log.info("Started topic monitor for base topic: {}", baseTopic);
    }

    private void startMonitoringCycle()
    {
        String topicPrefix = baseTopic + ".";
        timer.scheduleAtFixedRate(new TopicMonitorTask(), 0, 5000);

        while (running.get())
        {
            try
            {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e)
            {
                if (running.get())
                {
                    log.warn("Monitoring thread interrupted unexpectedly", e);
                }
                Thread.currentThread().interrupt();
            }
        }
    }

    private void interruptMonitoring()
    {
        if (timer != null)
        {
            timer.cancel();
            timer.purge();
        }
        if (adminClient != null)
        {
            adminClient.close(Duration.ofSeconds(5));
        }
        shutdownExecutorService();
    }

    private void shutdownExecutorService()
    {
        executorService.shutdown();
        try
        {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS))
            {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void cleanupResources()
    {
        try
        {
            if (adminClient != null)
            {
                adminClient.close(Duration.ofSeconds(5));
            }
        } catch (Exception e)
        {
            log.warn("Error closing admin client", e);
        }
    }

    private Set<String> detectNewTopics(Set<String> currentTopics)
    {
        return currentTopics.stream()
                .filter(t -> !subscribedTopics.contains(t))
                .collect(Collectors.toSet());
    }

    private String extractTableName(String topic)
    {
        int lastDotIndex = topic.lastIndexOf('.');
        return lastDotIndex != -1 ? topic.substring(lastDotIndex + 1) : topic;
    }

    private void launchConsumerTask(String topic)
    {
        try
        {
            TableMonitor task = new TableMonitor(kafkaProperties, topic);
            executorService.submit(task);
        } catch (Exception e)
        {
            log.error("Failed to start consumer for topic {}: {}", topic, e.getMessage());
        }
    }

    private class TopicMonitorTask extends TimerTask
    {
        @Override
        public void run()
        {
            if (!running.get())
            {
                cancel();
                return;
            }

            try
            {
                processTopicChanges();
            } catch (Exception e)
            {
                e.printStackTrace();
                log.error("Error processing topic changes: {}", e.getMessage());
            }
        }

        private void processTopicChanges()
        {
            try
            {
                ListTopicsResult listTopicsResult = adminClient.listTopics();
                Set<String> currentTopics = listTopicsResult.names().get(5, TimeUnit.SECONDS);
                Set<String> filteredTopics = filterTopics(currentTopics, baseTopic + ".");

                Set<String> newTopics = detectNewTopics(filteredTopics);
                handleNewTopics(newTopics);
            } catch (TimeoutException | ExecutionException | InterruptedException ignored)
            {

            }
        }

        private void handleNewTopics(Set<String> newTopics)
        {
            newTopics.stream()
                    .filter(this::shouldProcessTable)
                    .forEach(topic ->
                    {
                        try
                        {
                            TableMonitor task = new TableMonitor(kafkaProperties, topic);
                            executorService.submit(task);
                            activeTasks.put(topic, task);
                            subscribedTopics.add(topic);
                        } catch (IOException e)
                        {
                            log.error("Failed to create consumer for {}: {}", topic, e.getMessage());
                        }
                    });
        }

        private boolean shouldProcessTable(String topic)
        {
            String tableName = extractTableName(topic);
            return includeTables.length == 0 ||
                    Arrays.stream(includeTables).anyMatch(t -> t.equals(tableName));
        }
    }

}

