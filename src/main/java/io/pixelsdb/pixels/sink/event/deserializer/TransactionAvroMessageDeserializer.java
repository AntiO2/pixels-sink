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

package io.pixelsdb.pixels.sink.event.deserializer;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TransactionAvroMessageDeserializer implements Deserializer<SinkProto.TransactionMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionAvroMessageDeserializer.class);
    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer = new AvroKafkaDeserializer<>();
    private final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        enrichedConfig.put(SerdeConfig.CHECK_PERIOD_MS, SerdeConfig.CHECK_PERIOD_MS_DEFAULT);
        avroDeserializer.configure(enrichedConfig, isKey);
    }

    @Override
    public SinkProto.TransactionMetadata deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            MetricsFacade.getInstance().addRawData(bytes.length);
            GenericRecord avroRecord = avroDeserializer.deserialize(topic, bytes);
            return TransactionStructMessageDeserializer.convertToTransactionMetadata(avroRecord);
        } catch (Exception e) {
            logger.error("Avro deserialization failed for topic {}: {}", topic, e.getMessage());
            throw new SerializationException("Failed to deserialize Avro message", e);
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
