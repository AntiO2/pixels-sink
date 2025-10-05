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


package io.pixelsdb.pixels.sink.deserializer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @package: io.pixelsdb.pixels.sink.deserializer
 * @className: TransactionStructMessageDeserializer
 * @author: AntiO2
 * @date: 2025/9/26 12:42
 */
public class TransactionStructMessageDeserializer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionStructMessageDeserializer.class);

    @SuppressWarnings("unchecked")
    public static  <T> SinkProto.TransactionMetadata convertToTransactionMetadata(T record) {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();

        builder.setStatus(DeserializerUtil.getStatusSafely(record, "status"))
                .setId(DeserializerUtil.getTransIdPrefix(
                                DeserializerUtil.getStringSafely(record, "id")))
                .setEventCount(DeserializerUtil.getLongSafely(record, "event_count"))
                .setTimestamp(DeserializerUtil.getLongSafely(record, "ts_ms"));

        Object collections = DeserializerUtil.getFieldSafely(record, "data_collections");
        if (collections instanceof Iterable<?>) {
            for (Object item : (Iterable<?>) collections) {
                if (item instanceof GenericRecord collectionRecord) {
                    SinkProto.DataCollection.Builder collectionBuilder = SinkProto.DataCollection.newBuilder();
                    collectionBuilder.setDataCollection(
                            DeserializerUtil.getStringSafely(collectionRecord, "data_collection"));
                    collectionBuilder.setEventCount(
                            DeserializerUtil.getLongSafely(collectionRecord, "event_count"));
                    builder.addDataCollections(collectionBuilder);
                } else if (item instanceof Struct collectionRecord) {
                    SinkProto.DataCollection.Builder collectionBuilder = SinkProto.DataCollection.newBuilder();
                    collectionBuilder.setDataCollection(
                            DeserializerUtil.getStringSafely(collectionRecord, "data_collection"));
                    collectionBuilder.setEventCount(
                            DeserializerUtil.getLongSafely(collectionRecord, "event_count"));
                    builder.addDataCollections(collectionBuilder);
                }
            }
        }

        return builder.build();
    }
}
