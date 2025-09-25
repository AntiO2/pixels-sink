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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowChangeEventJsonDeserializer implements Deserializer<RowChangeEvent>
{
    private static final Logger logger = LoggerFactory.getLogger(RowChangeEventJsonDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();

    @Override
    public RowChangeEvent deserialize(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            logger.debug("Received empty message from topic: {}", topic);
            return null;
        }
        MetricsFacade.getInstance().addRawData(data.length);
        try
        {
            JsonNode rootNode = objectMapper.readTree(data);
            JsonNode payloadNode = rootNode.path("payload");

            SinkProto.OperationType opType = parseOperationType(payloadNode);

            return buildRowRecord(payloadNode, opType);
        } catch (Exception e)
        {
            logger.error("Failed to deserialize message from topic {}: {}", topic, e.getMessage());
            return null;
        }
    }

    private SinkProto.OperationType parseOperationType(JsonNode payloadNode)
    {
        String opCode = payloadNode.path("op").asText("");
        return DeserializerUtil.getOperationType(opCode);
    }

    @Deprecated
    private TypeDescription getSchema(JsonNode schemaNode, SinkProto.OperationType opType)
    {
        return switch (opType)
        {
            case DELETE -> SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "before");
            case INSERT, UPDATE, SNAPSHOT -> SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "after");
            case UNRECOGNIZED -> throw new IllegalArgumentException("Operation type is unknown. Check op");
        };
    }

    private RowChangeEvent buildRowRecord(JsonNode payloadNode,
                                          SinkProto.OperationType opType) throws SinkException
    {

        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();

        builder.setOp(parseOperationType(payloadNode))
                .setTsMs(payloadNode.path("ts_ms").asLong())
                .setTsUs(payloadNode.path("ts_us").asLong())
                .setTsNs(payloadNode.path("ts_ns").asLong());

        String schemaName;
        String tableName;
        if (payloadNode.has("source"))
        {
            SinkProto.SourceInfo.Builder sourceInfoBuilder = parseSourceInfo(payloadNode.get("source"));
            schemaName = sourceInfoBuilder.getDb(); // Notice we use the schema
            tableName = sourceInfoBuilder.getTable();
            builder.setSource(sourceInfoBuilder);
        } else
        {
            throw new IllegalArgumentException("Missing source field in row record");
        }

        TypeDescription typeDescription = tableMetadataRegistry.getTypeDescription(schemaName, tableName);
        RowDataParser rowDataParser = new RowDataParser(typeDescription);
        if (payloadNode.hasNonNull("transaction"))
        {
            builder.setTransaction(parseTransactionInfo(payloadNode.get("transaction")));
        }

        if (DeserializerUtil.hasBeforeValue(opType))
        {
            SinkProto.RowValue.Builder beforeBuilder = builder.getBeforeBuilder();
            rowDataParser.parse(payloadNode.get("before"), beforeBuilder);
            builder.setBefore(beforeBuilder);
        }

        if (DeserializerUtil.hasAfterValue(opType))
        {

            SinkProto.RowValue.Builder afterBuilder = builder.getAfterBuilder();
            rowDataParser.parse(payloadNode.get("after"), afterBuilder);
            builder.setBefore(afterBuilder);
        }

        RowChangeEvent event = new RowChangeEvent(builder.build(), typeDescription);
        try
        {
            event.initIndexKey();
        } catch (SinkException e)
        {
            logger.warn("Row change event {}: Init index key failed", event);
        }

        return event;
    }

    private SinkProto.SourceInfo.Builder parseSourceInfo(JsonNode sourceNode)
    {
        return SinkProto.SourceInfo.newBuilder()
                .setVersion(sourceNode.path("version").asText())
                .setConnector(sourceNode.path("connector").asText())
                .setName(sourceNode.path("name").asText())
                .setTsMs(sourceNode.path("ts_ms").asLong())
                .setSnapshot(sourceNode.path("snapshot").asText())
                .setDb(sourceNode.path("db").asText())
                .setSequence(sourceNode.path("sequence").asText())
                .setTsUs(sourceNode.path("ts_us").asLong())
                .setTsNs(sourceNode.path("ts_ns").asLong())
                .setSchema(sourceNode.path("schema").asText())
                .setTable(sourceNode.path("table").asText())
                .setTxId(sourceNode.path("txId").asLong())
                .setLsn(sourceNode.path("lsn").asLong())
                .setXmin(sourceNode.path("xmin").asLong());
    }

    private SinkProto.TransactionInfo parseTransactionInfo(JsonNode txNode)
    {
        return SinkProto.TransactionInfo.newBuilder()
                .setId(DeserializerUtil.getTransIdPrefix(txNode.path("id").asText()))
                .setTotalOrder(txNode.path("total_order").asLong())
                .setDataCollectionOrder(txNode.path("data_collection_order").asLong())
                .build();
    }

}

