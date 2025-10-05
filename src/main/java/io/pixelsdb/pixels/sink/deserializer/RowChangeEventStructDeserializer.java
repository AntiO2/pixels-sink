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


import com.fasterxml.jackson.databind.JsonNode;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.logging.Logger;

/**
 * @package: io.pixelsdb.pixels.sink.deserializer
 * @className: RowChangeEventStructDeserializer
 * @author: AntiO2
 * @date: 2025/9/26 12:00
 */
public class RowChangeEventStructDeserializer
{
    private static final Logger LOGGER = Logger.getLogger(RowChangeEventStructDeserializer.class.getName());
    private static final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();

    public static RowChangeEvent convertToRowChangeEvent(SourceRecord sourceRecord) throws SinkException
    {
        Struct value = (Struct) sourceRecord.value();
        String op = value.getString("op");
        SinkProto.OperationType operationType = DeserializerUtil.getOperationType(op);
        return buildRowRecord(value, operationType);
    }

    private static RowChangeEvent buildRowRecord(Struct value,
                                          SinkProto.OperationType opType) throws SinkException
    {

        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();

        builder.setOp(opType);

        String schemaName;
        String tableName;
        try {
            Struct source = value.getStruct("source");
            SinkProto.SourceInfo.Builder sourceInfoBuilder = parseSourceInfo(source);
            schemaName = sourceInfoBuilder.getDb(); // Notice we use the schema
            tableName = sourceInfoBuilder.getTable();
            builder.setSource(sourceInfoBuilder);
        } catch (DataException e)
        {
            LOGGER.warning("Missing source field in row record");
            throw new SinkException(e);
        }

        TypeDescription typeDescription = tableMetadataRegistry.getTypeDescription(schemaName, tableName);
        RowDataParser rowDataParser = new RowDataParser(typeDescription);

        try {
            Struct transaction = value.getStruct("transaction");
            SinkProto.TransactionInfo transactionInfo = parseTransactionInfo(transaction);
            builder.setTransaction(transactionInfo);
        } catch (DataException e)
        {
            LOGGER.warning("Missing transaction field in row record");
        }

        if (DeserializerUtil.hasBeforeValue(opType))
        {
            SinkProto.RowValue.Builder beforeBuilder = builder.getBeforeBuilder();
            rowDataParser.parse(value.getStruct("before"), beforeBuilder);
            builder.setBefore(beforeBuilder);
        }

        if (DeserializerUtil.hasAfterValue(opType))
        {

            SinkProto.RowValue.Builder afterBuilder = builder.getAfterBuilder();
            rowDataParser.parse(value.getStruct("after"), afterBuilder);
            builder.setAfter(afterBuilder);
        }

        RowChangeEvent event = new RowChangeEvent(builder.build(), typeDescription);
        return event;
    }

    private static  <T>  SinkProto.SourceInfo.Builder parseSourceInfo(T source) {
        return SinkProto.SourceInfo.newBuilder()
                .setVersion(DeserializerUtil.getStringSafely(source, "version"))
                .setConnector(DeserializerUtil.getStringSafely(source, "connector"))
                .setName(DeserializerUtil.getStringSafely(source, "name"))
                .setTsMs(DeserializerUtil.getLongSafely(source, "ts_ms"))
                .setSnapshot(DeserializerUtil.getStringSafely(source, "snapshot"))
                .setDb(DeserializerUtil.getStringSafely(source, "db"))
                .setSequence(DeserializerUtil.getStringSafely(source, "sequence"))
                .setTsUs(DeserializerUtil.getLongSafely(source, "ts_us"))
                .setTsNs(DeserializerUtil.getLongSafely(source, "ts_ns"))
                .setSchema(DeserializerUtil.getStringSafely(source, "schema"))
                .setTable(DeserializerUtil.getStringSafely(source, "table"))
                .setTxId(DeserializerUtil.getLongSafely(source, "txId"))
                .setLsn(DeserializerUtil.getLongSafely(source, "lsn"))
                .setXmin(DeserializerUtil.getLongSafely(source, "xmin"));
    }

    private static <T> SinkProto.TransactionInfo parseTransactionInfo(T txNode) {
        return SinkProto.TransactionInfo.newBuilder()
                .setId(DeserializerUtil.getTransIdPrefix(
                        DeserializerUtil.getStringSafely(txNode, "id")))
                .setTotalOrder(DeserializerUtil.getLongSafely(txNode, "total_order"))
                .setDataCollectionOrder(DeserializerUtil.getLongSafely(txNode, "data_collection_order"))
                .build();
    }
}
