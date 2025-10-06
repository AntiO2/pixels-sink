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
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.DateUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

class RowDataParser
{
    private final TypeDescription schema;

    public RowDataParser(TypeDescription schema)
    {
        this.schema = schema;
    }


    public void parse(GenericRecord record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseValue(record, fieldName, fieldType).build());
        }
    }

    public void parse(JsonNode node, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseValue(node.get(fieldName), fieldName, fieldType).build());
        }
    }


    public void parse(Struct record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            Field field = record.schema().field(fieldName);
            Schema.Type fieldType = field.schema().type();
            builder.addValues(parseValue(record.get(fieldName), fieldName, fieldType).build());
        }
    }
    private SinkProto.ColumnValue.Builder parseValue(JsonNode valueNode, String fieldName, TypeDescription type)
    {
        if (valueNode == null || valueNode.isNull())
        {
            return SinkProto.ColumnValue.newBuilder()
                    // .setName(fieldName)
                    .setValue(ByteString.EMPTY);
        }

        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();

        switch (type.getCategory())
        {
            case INT:
            {
                int value = valueNode.asInt();
                byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }
            case LONG:
            {
                long value = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }
            case CHAR:
            {
                String text = valueNode.asText();
                byte[] bytes = new byte[] { (byte) text.charAt(0) };
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case VARCHAR:
            case STRING:
            case VARBINARY:
            {
                String value = valueNode.asText().trim();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case DECIMAL:
            {
                String value = parseDecimal(valueNode, type).toString();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.DECIMAL)
//                        .setDimension(type.getPrecision())
//                        .setScale(type.getScale()));
                break;
            }
            case BINARY:
            {
                String base64 = valueNode.asText(); // assume already base64 encoded
                columnValueBuilder.setValue(ByteString.copyFrom(base64, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.BINARY));
                break;
            }
            case STRUCT:
            {
                // You can recursively parse fields in a struct here
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            }
            case DOUBLE:
            {
                double value = valueNode.asDouble();
                long longBits = Double.doubleToLongBits(value);
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(longBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.DOUBLE));
                break;
            }
            case FLOAT:
            {
                float value = (float) valueNode.asDouble();
                int intBits = Float.floatToIntBits(value);
                byte[] bytes = ByteBuffer.allocate(4).putInt(intBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.FLOAT));
                break;
            }
            case DATE:
            {
                int isoDate = valueNode.asInt();
                byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(isoDate).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            case TIMESTAMP:
            {
                long timestamp = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(timestamp).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + type.getCategory());
        }

        return columnValueBuilder;
    }


    @Deprecated // TODO: use bit
    private SinkProto.ColumnValue.Builder parseValue(GenericRecord record, String fieldName, TypeDescription fieldType)
    {
        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();
        // columnValueBuilder.setName(fieldName);

        Object raw = record.get(fieldName);
        if (raw == null)
        {
            columnValueBuilder.setValue(ByteString.EMPTY);
            return columnValueBuilder;
        }

        switch (fieldType.getCategory())
        {
            case INT:
            {
                int value = (int) raw;
                columnValueBuilder.setValue(ByteString.copyFrom(Integer.toString(value), StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }

            case LONG:
            {
                long value = (long) raw;
                columnValueBuilder.setValue(ByteString.copyFrom(Long.toString(value), StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }

            case STRING:
            {
                String value = raw.toString();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }

            case DECIMAL:
            {
                ByteBuffer buffer = (ByteBuffer) raw;
                String decimalStr = new String(buffer.array(), StandardCharsets.UTF_8).trim();
                columnValueBuilder.setValue(ByteString.copyFrom(decimalStr, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.DECIMAL)
//                        .setDimension(fieldType.getPrecision())
//                        .setScale(fieldType.getScale()));
                break;
            }

            case DATE:
            {
                int epochDay = (int) raw;
                String isoDate = LocalDate.ofEpochDay(epochDay).toString(); // e.g., "2025-07-03"
                columnValueBuilder.setValue(ByteString.copyFrom(isoDate, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.DATE));
                break;
            }

            case BINARY:
            {
                ByteBuffer buffer = (ByteBuffer) raw;
                // encode as hex or base64 if needed, otherwise just dump as UTF-8 string if it's meant to be readable
                String base64 = Base64.getEncoder().encodeToString(buffer.array());
                columnValueBuilder.setValue(ByteString.copyFrom(base64, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.BINARY));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldType.getCategory());
        }

        return columnValueBuilder;
    }

    private SinkProto.ColumnValue.Builder parseValue(Object record, String fieldName, Schema.Type type)
    {
        // TODO(AntiO2) support pixels type
        if (record == null)
        {
            return SinkProto.ColumnValue.newBuilder()
                    // .setName(fieldName)
                    .setValue(ByteString.EMPTY);
        }

        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();
        switch (type)
        {
            case INT8:
            case INT16:
            case INT32:
            {
                int value = (Integer) record;
                byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }
            case INT64:
            {
                long value = (Long) record;
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }
            case BYTES:
            {
                byte[] bytes = (byte[]) record;
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.BYTE));
                break;
            }
            case BOOLEAN:
            case STRING:
            {
                String value = (String) record;
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case STRUCT:
            {
                // You can recursively parse fields in a struct here
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            }
            case FLOAT64:
            {
                double value = (double) record;
                long doubleBits = Double.doubleToLongBits(value);
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(doubleBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.DOUBLE));
                break;
            }
            case FLOAT32:
            {
                float value = (float) record;
                int intBits = Float.floatToIntBits(value);
                byte[] bytes = ByteBuffer.allocate(4).putInt(intBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.FLOAT));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }

        return columnValueBuilder;
    }

    private Map<String, Object> parseDeleteRecord()
    {
        return Collections.singletonMap("__deleted", true);
    }

    BigDecimal parseDecimal(JsonNode node, TypeDescription type)
    {
        byte[] bytes = Base64.getDecoder().decode(node.asText());
        int scale = type.getScale();
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    private LocalDate parseDate(JsonNode node)
    {
        return LocalDate.ofEpochDay(node.asLong());
    }

    private byte[] parseBinary(JsonNode node)
    {
        try
        {
            return node.binaryValue();
        } catch (IOException e)
        {
            throw new RuntimeException("Binary parsing failed", e);
        }
    }
}