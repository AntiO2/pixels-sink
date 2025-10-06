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

package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import io.prometheus.client.Summary;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RowChangeEvent
{

    @Getter
    private final SinkProto.RowRecord rowRecord;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    @Getter
    private final TypeDescription schema;
    /**
     * timestamp from pixels transaction server
     */
    @Setter
    @Getter
    private long timeStamp;
    @Getter
    private String topic;
    @Getter
    private TableMetadata tableMetadata = null;
    private Summary.Timer latencyTimer;
    private Map<String, SinkProto.ColumnValue> beforeValueMap;
    private Map<String, SinkProto.ColumnValue> afterValueMap;
    @Getter
    private IndexProto.IndexKey beforeKey;
    @Getter
    private IndexProto.IndexKey afterKey;

    public RowChangeEvent(SinkProto.RowRecord rowRecord) throws SinkException
    {
        this.rowRecord = rowRecord;
        this.schema = TableMetadataRegistry.Instance().getTypeDescription(getSchemaName(), getTable());
        initColumnValueMap();
        initIndexKey();
    }

    public RowChangeEvent(SinkProto.RowRecord rowRecord, TypeDescription schema) throws SinkException
    {
        this.rowRecord = rowRecord;
        this.schema = schema;
        initColumnValueMap();
        // initIndexKey();
    }

    private void initColumnValueMap()
    {
        if (hasBeforeData())
        {
            this.beforeValueMap = new HashMap<>();
            initColumnValueMap(rowRecord.getBefore(), beforeValueMap);
        }

        if (hasAfterData())
        {
            this.afterValueMap = new HashMap<>();
            initColumnValueMap(rowRecord.getAfter(), afterValueMap);
        }
    }

    private void initColumnValueMap(SinkProto.RowValue rowValue, Map<String, SinkProto.ColumnValue> map)
    {
        IntStream.range(0, schema.getFieldNames().size())
                .forEach(i -> map.put(schema.getFieldNames().get(i), rowValue.getValuesList().get(i)));
    }

    public void initIndexKey() throws SinkException
    {
        this.tableMetadata = TableMetadataRegistry.Instance().getMetadata(
                this.rowRecord.getSource().getDb(),
                this.rowRecord.getSource().getTable());

        if (!this.tableMetadata.hasPrimaryIndex())
        {
            return;
        }
        if (hasBeforeData())
        {
            this.beforeKey = generateIndexKey(tableMetadata, beforeValueMap);
        }

        if (hasAfterData())
        {
            this.afterKey = generateIndexKey(tableMetadata, afterValueMap);
        }
    }

    private IndexProto.IndexKey generateIndexKey(TableMetadata tableMetadata, Map<String, SinkProto.ColumnValue> rowValue)
    {
        List<String> keyColumnNames = tableMetadata.getKeyColumnNames();
        SinglePointIndex index = tableMetadata.getIndex();
        int len = keyColumnNames.size();
        List<ByteString> keyColumnValues = new ArrayList<>(len);
        int keySize = 0;
        for (String keyColumnName : keyColumnNames)
        {
            ByteString value = rowValue.get(keyColumnName).getValue();
            keyColumnValues.add(value);
            keySize += value.size();
        }
        keySize += Long.BYTES + (len + 1) * 2; // table id + index key

        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);

        byteBuffer.putLong(index.getTableId()).putChar(':');
        for (ByteString value : keyColumnValues)
        {
            byteBuffer.put(value.toByteArray());
            byteBuffer.putChar(':');
        }

        return IndexProto.IndexKey.newBuilder()
                .setTimestamp(timeStamp)
                .setKey(ByteString.copyFrom(byteBuffer.rewind()))
                .setIndexId(index.getId())
                .setTableId(tableMetadata.getTable().getId())
                .build();
    }

    public String getSourceTable()
    {
        return rowRecord.getSource().getTable();
    }

    public SinkProto.TransactionInfo getTransaction()
    {
        return rowRecord.getTransaction();
    }

    public String getTable()
    {
        return rowRecord.getSource().getTable();
    }

    public String getFullTableName()
    {
        // TODO(AntiO2): In postgresql, data collection uses schemaName as prefix, while MySQL uses DB as prefix.
        return rowRecord.getSource().getSchema() + "." + rowRecord.getSource().getTable();
        // return getSchemaName() + "." + getTable();
    }

    // TODO(AntiO2): How to Map Schema Names Between Source DB and Pixels
    public String getSchemaName()
    {
        return rowRecord.getSource().getDb();
        // return rowRecord.getSource().getSchema();
    }

    public boolean hasError()
    {
        return false;
    }

    public String getDb()
    {
        return rowRecord.getSource().getDb();
    }

    public boolean isDelete()
    {
        return getOp() == SinkProto.OperationType.DELETE;
    }

    public boolean isInsert()
    {
        return getOp() == SinkProto.OperationType.INSERT;
    }

    public boolean isSnapshot()
    {
        return getOp() == SinkProto.OperationType.SNAPSHOT;
    }

    public boolean isUpdate()
    {
        return getOp() == SinkProto.OperationType.UPDATE;
    }

    public boolean hasBeforeData()
    {
        return isUpdate() || isDelete();
    }

    public boolean hasAfterData()
    {
        return isUpdate() || isInsert() || isSnapshot();
    }

    public void startLatencyTimer()
    {
        this.latencyTimer = metricsFacade.startProcessLatencyTimer();
    }

    public void endLatencyTimer()
    {
        if (latencyTimer != null)
        {
            this.latencyTimer.close();
        }

    }

    public SinkProto.OperationType getOp()
    {
        return rowRecord.getOp();
    }

    public SinkProto.RowValue getBefore()
    {
        return rowRecord.getBefore();
    }

    public SinkProto.RowValue getAfter()
    {
        return rowRecord.getAfter();
    }

    public List<ByteString> getAfterData()
    {
        List<SinkProto.ColumnValue> colValues = rowRecord.getAfter().getValuesList();
        List<ByteString> colValueList = new ArrayList<>(colValues.size());
        for (SinkProto.ColumnValue col : colValues)
        {
            colValueList.add(col.getValue());
        }
        return colValueList;
    }

    @Override
    public String toString()
    {
        String sb = "RowChangeEvent{" +
                rowRecord.getSource().getDb() +
                "." + rowRecord.getSource().getTable() +
                rowRecord.getTransaction().getId();
        return sb;
    }
}
