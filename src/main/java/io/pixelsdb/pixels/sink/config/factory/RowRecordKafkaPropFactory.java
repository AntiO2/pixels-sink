/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
 
package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class RowRecordKafkaPropFactory implements KafkaPropFactory
{
    static Properties getCommonKafkaProperties(PixelsSinkConfig config)
    {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProperties;
    }

    @Override
    public Properties createKafkaProperties(PixelsSinkConfig config)
    {
        Properties kafkaProperties = getCommonKafkaProperties(config);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());

        kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        kafkaProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        return kafkaProperties;
    }
}
