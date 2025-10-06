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


package io.pixelsdb.pixels.sink.sink;


import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.ProtoType;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * @package: io.pixelsdb.pixels.sink.sink
 * @className: ProtoWriter
 * @author: AntiO2
 * @date: 2025/10/5 07:10
 */
public class ProtoWriter implements PixelsSinkWriter
{
    private final Logger LOGGER = LoggerFactory.getLogger(ProtoWriter.class);
    private final RotatingWriterManager writerManager;
    private final TableMetadataRegistry instance;

    public ProtoWriter() throws IOException
    {
        PixelsSinkConfig sinkConfig = PixelsSinkConfigFactory.getInstance();

        String dataPath = sinkConfig.getSinkProtoData();
        this.writerManager =  new RotatingWriterManager(dataPath);
        this.instance = TableMetadataRegistry.Instance();
    }

    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
    {
        byte[] transData = transactionMetadata.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(ProtoType.TRANS.toInt());
        return writeData(buffer.array(), transData);
    }

    private boolean writeData(byte[] key, byte[] data)
    {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + key.length + data.length).order(ByteOrder.BIG_ENDIAN); // rowLen + type + data

        buf.putInt(key.length).putInt(data.length).put(key).put(data);
        PhysicalWriter writer;
        try {
            writer = writerManager.current();
            writer.prepare(buf.remaining());
            writer.append(buf.array());
        } catch (IOException e)
        {
            LOGGER.error("Error while writing row record.", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean write(RowChangeEvent rowChangeEvent)
    {
        return write(rowChangeEvent.getRowRecord());
    }

    public boolean write(SinkProto.RowRecord rowRecord)
    {
        byte[] rowData = rowRecord.toByteArray();
        String tableName = rowRecord.getSource().getTable();
        String schemaName = rowRecord.getSource().getDb();

//        long tableId;
//        try
//        {
//            tableId = instance.getTableId(schemaName, tableName);
//        } catch (SinkException e)
//        {
//            LOGGER.error("Error while getting schema table id.", e);
//            return false;
//        }
//
//        ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
//        keyBuffer.putInt(ProtoType.ROW.toInt())
//                .putLong(tableId);

        byte[] schemaNameBytes = schemaName.getBytes();
        byte[] tableNameBytes = tableName.getBytes();

        ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES * 3 + schemaNameBytes.length + tableNameBytes.length);
        keyBuffer.putInt(ProtoType.ROW.toInt()).putInt(schemaNameBytes.length).putInt(tableNameBytes.length);
        keyBuffer.put(schemaNameBytes).put(tableNameBytes);
        return writeData(keyBuffer.array(), rowData);
    }

    @Override
    public void flush()
    {

    }


    @Override
    public void close() throws IOException
    {
        this.writerManager.close();
    }

    @Override
    public boolean writeTrans(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData, long timestamp)
    {
        return false;
    }

    @Override
    public boolean writeBatch(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData)
    {
        return false;
    }
}
