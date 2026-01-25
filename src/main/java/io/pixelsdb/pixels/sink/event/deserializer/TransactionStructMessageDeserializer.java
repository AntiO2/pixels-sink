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


import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @package: io.pixelsdb.pixels.sink.event.deserializer
 * @className: TransactionStructMessageDeserializer
 * @author: AntiO2
 * @date: 2025/9/26 12:42
 */
public class TransactionStructMessageDeserializer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionStructMessageDeserializer.class);

    @SuppressWarnings("unchecked")
    public static <T> SinkProto.TransactionMetadata convertToTransactionMetadata(T record)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();

        builder.setStatus(DeserializerUtil.getStatusSafely(record, "status"))
                .setId(DeserializerUtil.getTransIdPrefix(
                        DeserializerUtil.getStringSafely(record, "id")))
                .setEventCount(DeserializerUtil.getLongSafely(record, "event_count"))
                .setTimestamp(DeserializerUtil.getLongSafely(record, "ts_ms"));

        Object collections = DeserializerUtil.getFieldSafely(record, "data_collections");
        if (collections instanceof Iterable<?>)
        {
            for (Object item : (Iterable<?>) collections)
            {
                if (item instanceof GenericRecord collectionRecord)
                {
                    SinkProto.DataCollection.Builder collectionBuilder = SinkProto.DataCollection.newBuilder();
                    collectionBuilder.setDataCollection(
                            DeserializerUtil.getStringSafely(collectionRecord, "data_collection"));
                    collectionBuilder.setEventCount(
                            DeserializerUtil.getLongSafely(collectionRecord, "event_count"));
                    builder.addDataCollections(collectionBuilder);
                } else if (item instanceof Struct collectionRecord)
                {
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
