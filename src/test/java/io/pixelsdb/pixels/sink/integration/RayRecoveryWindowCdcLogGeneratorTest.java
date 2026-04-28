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

package io.pixelsdb.pixels.sink.integration;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.proto.ProtoWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;

class RayRecoveryWindowCdcLogGeneratorTest
{
    private static final String TOPIC = "ray-recovery-window-it-v2";
    private static final String SCHEMA = "pixels_test";
    private static final String TABLE = "ray";
    private static final String TX_ID = "ray-window-tx-1";
    private static final int ROW_COUNT = 50;
    private static final int START_ID = 910000;

    @AfterEach
    void tearDown()
    {
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldGenerateLongRunningRecoveryWindowLog() throws Exception
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("sink.proto.dir", "file:///home/antio2/projects/pixels-sink/target/integration-tests/proto-cdc");
        configFactory.addProperty("sink.proto.data", TOPIC);
        configFactory.addProperty("sink.proto.maxRecords", "1000");
        PixelsSinkConfigFactory.initialize(configFactory);

        try (ProtoWriter writer = new ProtoWriter())
        {
            writer.writeTrans(txBegin(TX_ID));
            for (int i = 0; i < ROW_COUNT; i++)
            {
                int id = START_ID + i;
                int age = 20 + (i % 10);
                String ts = String.format("2026-04-29 03:%02d:%02d", i / 60, i % 60);
                writer.writeRow(insert(TX_ID, id, age, ts));
            }
            writer.writeTrans(txEnd(TX_ID, ROW_COUNT));
        }
    }

    private SinkProto.TransactionMetadata txBegin(String txId)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.BEGIN)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private SinkProto.TransactionMetadata txEnd(String txId, int rowCount)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.END)
                .setTimestamp(System.currentTimeMillis())
                .addDataCollections(SinkProto.DataCollection.newBuilder()
                        .setDataCollection(SCHEMA + "." + TABLE)
                        .setEventCount(rowCount)
                        .build())
                .build();
    }

    private RowChangeEvent insert(String txId, int id, int age, String freshnessTs) throws Exception
    {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder()
                .setSource(source())
                .setTransaction(transaction(txId))
                .setOp(SinkProto.OperationType.INSERT)
                .setAfter(rowValue(id, age, freshnessTs))
                .build());
    }

    private SinkProto.SourceInfo source()
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(SCHEMA)
                .setSchema(SCHEMA)
                .setTable(TABLE)
                .build();
    }

    private SinkProto.TransactionInfo transaction(String txId)
    {
        return SinkProto.TransactionInfo.newBuilder()
                .setId(txId)
                .build();
    }

    private SinkProto.RowValue rowValue(int id, int age, String freshnessTs)
    {
        return SinkProto.RowValue.newBuilder()
                .addValues(intColumnValue(id))
                .addValues(intColumnValue(age))
                .addValues(timestampColumnValue(freshnessTs))
                .build();
    }

    private SinkProto.ColumnValue intColumnValue(int value)
    {
        return SinkProto.ColumnValue.newBuilder()
                .setValue(ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()))
                .build();
    }

    private SinkProto.ColumnValue timestampColumnValue(String value)
    {
        long epochMillis = LocalDateTime.parse(value.replace(" ", "T"))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        return SinkProto.ColumnValue.newBuilder()
                .setValue(ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(epochMillis).array()))
                .build();
    }
}
