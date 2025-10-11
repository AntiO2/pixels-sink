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


package io.pixelsdb.pixels.sink.writer;


import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.storage.localfs.PhysicalLocalReader;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * @package: io.pixelsdb.pixels.sink.writer
 * @className: TestProtoWriter
 * @author: AntiO2
 * @date: 2025/10/5 09:24
 */
public class TestProtoWriter
{
    public static String schemaName = "test";
    public static String tableName = "ray";

    @BeforeAll
    public static void setUp() throws IOException
    {
        PixelsSinkConfigFactory.initialize("/home/pixels/projects/pixels-writer/src/main/resources/pixels-writer.local.properties");
//        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-writer/src/main/resources/pixels-writer.aws.properties");
    }

    private static SinkProto.RowRecord getRowRecord(int i)
    {
        byte[][] cols = new byte[3][];

        cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
        cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
        cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);
        SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder();
        afterValueBuilder
                .addValues(
                        SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).build())
                .addValues(
                        SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).build())
                .addValues(
                        SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[2]))).build());


        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
        builder.setOp(SinkProto.OperationType.INSERT)
                .setAfter(afterValueBuilder)
                .setSource(
                        SinkProto.SourceInfo.newBuilder()
                                .setDb(schemaName)
                                .setTable(tableName)
                                .build()
                );
        return builder.build();
    }

    private static SinkProto.TransactionMetadata getTrans(int i, SinkProto.TransactionStatus status)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setId(Integer.toString(i));
        builder.setStatus(status);
        builder.setTimestamp(System.currentTimeMillis());
        return builder.build();
    }

    @SneakyThrows
    @Test
    public void testWriteTransInfo()
    {
        ProtoWriter transWriter = new ProtoWriter();
        int maxTx = 1000;

        for (int i = 0; i < maxTx; i++)
        {
            transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.BEGIN));
            transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.END));
        }
        transWriter.close();
    }

    @Test
    public void testWriteFile() throws IOException
    {
        String path = "/home/pixels/projects/pixels-writer/tmp/write.dat";
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(Storage.Scheme.file, path);

        int writeNum = 3;

        ByteBuffer buf = ByteBuffer.allocate(writeNum * Integer.BYTES);
        for (int i = 0; i < 3; i++)
        {
            buf.putInt(i);
        }
        writer.append(buf);
        writer.close();
    }

    @Test
    public void testReadFile() throws IOException
    {
        String path = "/home/pixels/projects/pixels-writer/tmp/write.dat";
        PhysicalLocalReader reader = (PhysicalLocalReader) PhysicalReaderUtil.newPhysicalReader(Storage.Scheme.file, path);

        int writeNum = 12;
        for (int i = 0; i < writeNum; i++)
        {
            reader.readLong(ByteOrder.BIG_ENDIAN);
        }
    }

    @Test
    public void testReadEmptyFile() throws IOException
    {
        String path = "/home/pixels/projects/pixels-writer/tmp/empty.dat";
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(Storage.Scheme.file, path);

        int v = reader.readInt(ByteOrder.BIG_ENDIAN);

        return;
    }

    @Test
    public void testWriteRowInfo() throws IOException
    {
        ProtoWriter transWriter = new ProtoWriter();
        int maxTx = 10000000;
        int rowCnt = 0;
        for (int i = 0; i < maxTx; i++)
        {
            transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.BEGIN));
            for (int j = i; j < 3; j++)
            {
                transWriter.write(getRowRecord(rowCnt++));
            }
            transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.END));
        }
        transWriter.close();
    }
}
